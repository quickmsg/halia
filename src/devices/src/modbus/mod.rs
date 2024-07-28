use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use message::MessageBatch;
use point::Point;
use protocol::modbus::{
    client::{rtu, tcp, Context, Writer},
    SlaveContext,
};
use serde_json::json;
use sink::Sink;
use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tokio_serial::{DataBits, Parity, SerialPort, SerialStream, StopBits};
use tracing::{debug, warn};
use types::{
    devices::{
        modbus::{
            Area, CreateUpdateModbusReq, CreateUpdatePointReq, CreateUpdateSinkReq, DataType,
            Encode, Endian, ModbusConf, SearchPointsResp, SearchSinksResp, Type,
        },
        SearchDevicesItemResp,
    },
    Pagination, Value,
};
use uuid::Uuid;

pub const TYPE: &str = "modbus";
pub mod manager;
mod point;
mod sink;

#[derive(Debug)]
pub struct Modbus {
    pub id: Uuid,
    conf: CreateUpdateModbusReq,

    points: Arc<RwLock<Vec<Point>>>,
    sinks: Vec<Sink>,

    on: bool,
    err: Option<String>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    rtt: Arc<AtomicU16>,
    write_tx: Option<mpsc::Sender<WritePointEvent>>,
    read_tx: Option<mpsc::Sender<Uuid>>,

    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Receiver<WritePointEvent>,
            mpsc::Receiver<Uuid>,
        )>,
    >,
}

impl Modbus {
    pub async fn new(device_id: Option<Uuid>, req: CreateUpdateModbusReq) -> HaliaResult<Modbus> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::modbus::create(
                &device_id,
                TYPE,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Modbus {
            id: device_id,
            on: false,
            err: None,
            rtt: Arc::new(AtomicU16::new(9999)),
            conf: req,
            points: Arc::new(RwLock::new(vec![])),
            read_tx: None,
            write_tx: None,
            sinks: vec![],
            stop_signal_tx: None,
            join_handle: None,
        })
    }

    pub async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::devices::modbus::read_points(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);

                    let point_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdatePointReq = serde_json::from_str(items[1])?;
                    self.create_point(Some(point_id), req).await?;
                }
            }
            Err(e) => return Err(e.into()),
        }

        match persistence::devices::modbus::read_sinks(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);

                    let sink_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdateSinkReq = serde_json::from_str(items[1])?;
                    self.create_sink(Some(sink_id), req).await?;
                }
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    pub fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id.clone(),
            r#type: TYPE,
            rtt: self.rtt.load(Ordering::SeqCst),
            on: self.stop_signal_tx.is_some(),
            err: self.err.clone(),
            conf: json!(&self.conf),
        }
    }

    pub async fn update(&mut self, req: CreateUpdateModbusReq) -> HaliaResult<()> {
        persistence::devices::update_device_conf(&self.id, serde_json::to_string(&req).unwrap())
            .await?;

        let mut restart = false;
        if self.conf.modbus != req.modbus {
            restart = true;
        }

        self.conf = req;
        if restart && self.stop_signal_tx.is_some() {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (stop_signal_rx, write_rx, read_rx) =
                self.join_handle.take().unwrap().await.unwrap();

            self.event_loop(stop_signal_rx, read_rx, write_rx).await;
        }

        Ok(())
    }

    pub async fn start(&mut self) -> HaliaResult<()> {
        if self.on {
            return Ok(());
        } else {
            self.on = true;
        }
        debug!("设备开启");

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (read_tx, read_rx) = mpsc::channel::<Uuid>(16);
        let (write_tx, write_rx) = mpsc::channel::<WritePointEvent>(16);
        for point in self.points.write().await.iter_mut() {
            point.start(read_tx.clone()).await;
        }
        for sink in self.sinks.iter_mut() {
            sink.start(write_tx.clone()).await;
        }

        self.read_tx = Some(read_tx);
        self.write_tx = Some(write_tx);
        self.event_loop(stop_signal_rx, read_rx, write_rx).await;

        Ok(())
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut read_rx: mpsc::Receiver<Uuid>,
        mut write_rx: mpsc::Receiver<WritePointEvent>,
    ) {
        let modbus_conf = self.conf.modbus.clone();
        let interval = self.conf.modbus.interval;
        let points = self.points.clone();
        let reconnect = self.conf.modbus.reconnect;

        let rtt = self.rtt.clone();
        let read_tx = self.read_tx.as_ref().unwrap().clone();
        let handle = tokio::spawn(async move {
            loop {
                match Modbus::connect(&modbus_conf).await {
                    Ok(mut ctx) => {
                        for point in points.write().await.iter_mut() {
                            point.start(read_tx.clone()).await;
                        }
                        loop {
                            select! {
                                biased;
                                _ = stop_signal_rx.recv() => {
                                    return (stop_signal_rx, write_rx, read_rx);
                                }

                                wpe = write_rx.recv() => {
                                    debug!("here");
                                    if let Some(wpe) = wpe {
                                        if write_value(&mut ctx, wpe).await.is_err() {
                                            break
                                        }
                                    }
                                    if interval > 0 {
                                        time::sleep(Duration::from_millis(interval)).await;
                                    }
                                }

                                point_id = read_rx.recv() => {
                                   if let Some(point_id) = point_id {
                                        match points.write().await.iter_mut().find(|point| point.id == point_id) {
                                            Some(point) => {
                                                let now = Instant::now();
                                                if let Err(_) = point.read(&mut ctx).await {
                                                    break
                                                }
                                                rtt.store(now.elapsed().as_secs() as u16, Ordering::SeqCst);
                                            }
                                            None => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!("{}", e);
                        for point in points.write().await.iter_mut() {
                            point.stop().await;
                        }

                        let sleep = time::sleep(Duration::from_secs(reconnect));
                        tokio::pin!(sleep);
                        select! {
                            _ = stop_signal_rx.recv() => {
                                return (stop_signal_rx, write_rx, read_rx);
                            }

                            _ = &mut sleep => {}
                        }
                    }
                }
            }
        });
        self.join_handle = Some(handle);
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if !self.on {
            return Ok(());
        } else {
            self.on = false;
        }
        debug!("设备停止");

        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        for point in self.points.write().await.iter_mut() {
            point.stop().await;
        }
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();

        self.stop_signal_tx = None;
        self.read_tx = None;
        self.write_tx = None;
        self.join_handle = None;

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if self.stop_signal_tx.is_some() {
            return Err(HaliaError::DeviceRunning);
        }
        debug!("设备删除");
        persistence::devices::delete_device(&self.id).await?;
        Ok(())
    }

    async fn connect(conf: &ModbusConf) -> HaliaResult<Context> {
        match conf.link_type {
            types::devices::modbus::LinkType::Ethernet => {
                let ethernet = conf.ethernet.as_ref().unwrap();
                let socket_addr: SocketAddr = format!("{}:{}", ethernet.host, ethernet.port)
                    .parse()
                    .unwrap();
                let transport = TcpStream::connect(socket_addr).await?;
                match ethernet.encode {
                    Encode::Tcp => Ok(tcp::attach(transport)),
                    Encode::RtuOverTcp => Ok(rtu::attach(transport)),
                }
            }
            types::devices::modbus::LinkType::Serial => {
                let serial = conf.serial.as_ref().unwrap();
                let builder = tokio_serial::new(serial.path.clone(), serial.baud_rate);
                let mut port = SerialStream::open(&builder).unwrap();
                match serial.stop_bits {
                    1 => port.set_stop_bits(StopBits::One).unwrap(),
                    2 => port.set_stop_bits(StopBits::Two).unwrap(),
                    _ => unreachable!(),
                };
                match serial.data_bits {
                    5 => port.set_data_bits(DataBits::Five).unwrap(),
                    6 => port.set_data_bits(DataBits::Six).unwrap(),
                    7 => port.set_data_bits(DataBits::Seven).unwrap(),
                    8 => port.set_data_bits(DataBits::Eight).unwrap(),
                    _ => unreachable!(),
                };

                match serial.parity {
                    0 => port.set_parity(Parity::None).unwrap(),
                    1 => port.set_parity(Parity::Odd).unwrap(),
                    2 => port.set_parity(Parity::Even).unwrap(),
                    _ => unreachable!(),
                };

                Ok(rtu::attach(port))
            }
        }
    }

    pub async fn create_point(
        &mut self,
        point_id: Option<Uuid>,
        req: CreateUpdatePointReq,
    ) -> HaliaResult<()> {
        match Point::new(&self.id, point_id, req).await {
            Ok(mut point) => {
                // TODO 错误判断
                if self.on {
                    point.start(self.read_tx.as_ref().unwrap().clone()).await;
                }
                self.points.write().await.push(point);
                Ok(())
            }
            Err(_) => todo!(),
        }
    }

    pub async fn search_points(&self, pagination: Pagination) -> SearchPointsResp {
        let mut data = vec![];
        for point in self
            .points
            .read()
            .await
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(point.search());
            if data.len() == pagination.size {
                break;
            }
        }

        SearchPointsResp {
            total: self.points.read().await.len(),
            data,
        }
    }

    pub async fn update_point(
        &mut self,
        point_id: Uuid,
        req: CreateUpdatePointReq,
    ) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == point_id)
        {
            Some(point) => point.update(&self.id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn write_point_value(&self, point_id: Uuid, value: Value) -> HaliaResult<()> {
        if self.stop_signal_tx.is_none() {
            return Err(HaliaError::DeviceStoped);
        }
        if self.err.is_some() {
            return Err(HaliaError::DeviceDisconnect);
        }

        match self
            .points
            .read()
            .await
            .iter()
            .find(|point| point.id == point_id)
        {
            Some(point) => {
                match WritePointEvent::new(
                    point.conf.point.slave,
                    point.conf.point.area.clone(),
                    point.conf.point.address,
                    point.conf.point.data_type.clone(),
                    value.value,
                ) {
                    Ok(wpe) => {
                        let _ = self.write_tx.as_ref().unwrap().send(wpe).await;
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_point(&mut self, point_id: Uuid) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == point_id)
        {
            Some(point) => point.delete(&self.id).await?,
            None => return Err(HaliaError::NotFound),
        }

        self.points
            .write()
            .await
            .retain(|point| point.id != point_id);
        Ok(())
    }

    pub async fn add_subscribe_ref(&mut self, point_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == *point_id)
        {
            Some(point) => Ok(point.add_ref(rule_id)),
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn subscribe(
        &mut self,
        point_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == *point_id)
        {
            Some(point) => {
                if !point.on {
                    return Err(HaliaError::DeviceStoped);
                }
                Ok(point.subscribe(rule_id))
            }
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn unsubscribe(&mut self, point_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == *point_id)
        {
            Some(point) => Ok(point.unsubscribe(rule_id)),
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn remove_subscribe_ref(
        &mut self,
        point_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == *point_id)
        {
            Some(point) => Ok(point.remove_ref(rule_id)),
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(&self.id, sink_id, req).await {
            Ok(mut sink) => {
                if self.on {
                    sink.start(self.write_tx.as_ref().unwrap().clone()).await;
                }
                Ok(self.sinks.push(sink))
            }
            Err(e) => Err(e),
        }
    }

    pub async fn search_sinks(&self, pagination: Pagination) -> SearchSinksResp {
        let mut data = vec![];
        for sink in self
            .sinks
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(sink.search());
            if data.len() == pagination.size {
                break;
            }
        }

        SearchSinksResp {
            total: self.sinks.len(),
            data,
        }
    }

    pub async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.update(&self.id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub fn pre_publish(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.pre_publish(rule_id)),
            None => Err(HaliaError::NotFound),
        }
    }

    pub fn pre_unpublish(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.pre_unpublish(rule_id)),
            None => Err(HaliaError::NotFound),
        }
    }

    pub fn publish(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.publish(rule_id)),
            None => Err(HaliaError::NotFound),
        }
    }

    pub fn unpublish(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.unpublish(rule_id)),
            None => Err(HaliaError::NotFound),
        }
    }
}

#[derive(Debug)]
pub struct WritePointEvent {
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub data_type: DataType,
    pub data: Vec<u8>,
}

impl WritePointEvent {
    pub fn new(
        slave: u8,
        area: Area,
        address: u16,
        data_type: DataType,
        value: serde_json::Value,
    ) -> HaliaResult<Self> {
        match area {
            Area::InputDiscrete | Area::InputRegisters => {
                return Err(HaliaError::DevicePointNotSupportWriteMethod)
            }
            _ => {}
        }

        let data = match data_type.encode(value) {
            Ok(data) => data,
            Err(e) => {
                debug!("encode value err :{}", e);
                return Err(HaliaError::DevicePointWriteValueErr);
            }
        };

        Ok(WritePointEvent {
            slave,
            area,
            address,
            data_type,
            data,
        })
    }
}

async fn write_value(ctx: &mut Context, wpe: WritePointEvent) -> HaliaResult<()> {
    debug!("{:?}", wpe);
    ctx.set_slave(wpe.slave);
    match wpe.area {
        Area::Coils => match wpe.data_type.typ {
            Type::Bool => match ctx.write_single_coil(wpe.address, wpe.data[0]).await {
                Ok(res) => match res {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        warn!("modbus protocl exception:{}", e);
                        return Ok(());
                    }
                },
                Err(e) => {
                    debug!("{}", e);
                    return Err(HaliaError::ConfErr);
                }
            },
            _ => return Err(HaliaError::ConfErr),
        },
        Area::HoldingRegisters => match wpe.data_type.typ {
            Type::Bool => {
                let pos = wpe.data_type.pos.as_ref().unwrap();
                let and_mask = !(1 << pos);
                let or_mask = (wpe.data[0] as u16) << pos;
                match ctx
                    .masked_write_register(wpe.address, and_mask, or_mask)
                    .await
                {
                    Ok(res) => match res {
                        Ok(_) => return Ok(()),
                        Err(_) => {
                            // todo log error
                            return Ok(());
                        }
                    },
                    Err(e) => return Err(HaliaError::DeviceConnectErr(e.to_string())),
                }
            }
            Type::Int8 | Type::Uint8 => {
                let (and_mask, or_mask) = match wpe.data_type.single_endian.as_ref().unwrap() {
                    Endian::Little => (0x00FF, (wpe.data[0] as u16) << 8),
                    Endian::Big => (0xFF00, wpe.data[0] as u16),
                };
                match ctx
                    .masked_write_register(wpe.address, and_mask, or_mask)
                    .await
                {
                    Ok(res) => match res {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            warn!("modbus protocol exception:{}", e);
                            return Ok(());
                        }
                    },
                    Err(e) => return Err(HaliaError::DeviceConnectErr(e.to_string())),
                }
            }
            Type::Int16 | Type::Uint16 => {
                match ctx.write_single_register(wpe.address, &wpe.data).await {
                    Ok(res) => match res {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            warn!("modbus protocol exception:{}", e);
                            return Ok(());
                        }
                    },
                    Err(e) => return Err(HaliaError::DeviceConnectErr(e.to_string())),
                }
            }
            Type::Int32
            | Type::Uint32
            | Type::Int64
            | Type::Uint64
            | Type::Float32
            | Type::Float64
            | Type::String
            | Type::Bytes => match ctx.write_multiple_registers(wpe.address, &wpe.data).await {
                Ok(res) => match res {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        warn!("modbus protocol exception:{}", e);
                        return Ok(());
                    }
                },
                Err(e) => return Err(HaliaError::DeviceConnectErr(e.to_string())),
            },
        },
        _ => return Err(HaliaError::ConfErr),
    }
}
