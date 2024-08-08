use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id,
    persistence::{self, Status},
};
use message::MessageBatch;
use point::Point;
use protocol::modbus::{rtu, tcp, Context};
use sink::Sink;
use std::{
    io,
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
use tracing::{trace, warn};
use types::{
    devices::{
        modbus::{
            Area, CreateUpdateModbusReq, CreateUpdatePointReq, CreateUpdateSinkReq, DataType,
            Encode, ModbusConf, SearchPointsResp, SearchSinksResp, Type,
        },
        SearchDevicesItemConf, SearchDevicesItemResp,
    },
    Pagination, Value,
};
use uuid::Uuid;

pub const TYPE: &str = "modbus";
pub mod manager;
mod point;
mod sink;

fn point_not_find_error(point_id: Uuid) -> HaliaError {
    HaliaError::NotFound("点位".to_owned(), point_id)
}

fn sink_not_find_error(sink_id: Uuid) -> HaliaError {
    HaliaError::NotFound("动作".to_owned(), sink_id)
}

#[derive(Debug)]
pub struct Modbus {
    pub id: Uuid,

    conf: CreateUpdateModbusReq,

    points: Arc<RwLock<Vec<Point>>>,
    sinks: Vec<Sink>,

    on: bool,
    err: Arc<RwLock<Option<String>>>,
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
        let (device_id, new) = get_id(device_id);

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
            err: Arc::new(RwLock::new(None)),
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
                    // TODO not collect
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

    pub async fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id.clone(),
            typ: TYPE,
            rtt: self.rtt.load(Ordering::SeqCst),
            on: self.on,
            err: self.err.read().await.clone(),
            conf: SearchDevicesItemConf {
                base: self.conf.base.clone(),
                ext: serde_json::json!(self.conf.ext),
            },
        }
    }

    pub async fn update(&mut self, req: CreateUpdateModbusReq) -> HaliaResult<()> {
        persistence::devices::update_device_conf(&self.id, serde_json::to_string(&req).unwrap())
            .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }

        self.conf = req;
        if restart && self.on {
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
        check_and_set_on_true!(self);
        trace!("设备开启");

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (read_tx, read_rx) = mpsc::channel::<Uuid>(16);
        let (write_tx, write_rx) = mpsc::channel::<WritePointEvent>(16);
        for point in self.points.write().await.iter_mut() {
            point.start(read_tx.clone(), self.err.clone()).await;
        }
        for sink in self.sinks.iter_mut() {
            sink.start(write_tx.clone(), self.err.clone()).await;
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
        let modbus_conf = self.conf.ext.clone();
        let interval = self.conf.ext.interval;
        let points = self.points.clone();
        let reconnect = self.conf.ext.reconnect;

        let err = self.err.clone();

        let rtt = self.rtt.clone();
        let handle = tokio::spawn(async move {
            loop {
                match Modbus::connect(&modbus_conf).await {
                    Ok(mut ctx) => {
                        *err.write().await = None;
                        loop {
                            select! {
                                biased;
                                _ = stop_signal_rx.recv() => {
                                    return (stop_signal_rx, write_rx, read_rx);
                                }

                                wpe = write_rx.recv() => {
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
                        *err.write().await = Some(e.to_string());
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
        check_and_set_on_false!(self);

        for point in self.points.read().await.iter() {
            if !point.can_stop() {
                return Err(HaliaError::Common("设备有源被运行规则引用中".to_owned()));
            }
        }

        for sink in &self.sinks {
            if !sink.can_stop() {
                return Err(HaliaError::Common("设备有动作被运行规则引用中".to_owned()));
            }
        }
        trace!("停止");

        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        for point in self.points.write().await.iter_mut() {
            point.stop().await;
        }

        for sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();

        self.stop_signal_tx = None;
        *self.err.write().await = None;
        self.read_tx = None;
        self.write_tx = None;
        self.join_handle = None;

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if self.on {
            return Err(HaliaError::Running);
        }
        for point in self.points.read().await.iter() {
            if !point.can_delete() {
                return Err(HaliaError::Common("设备点位被规则引用中".to_owned()));
            }
        }

        for sink in &self.sinks {
            if !sink.can_delete() {
                return Err(HaliaError::Common("设备动作被规则引用中".to_owned()));
            }
        }
        trace!("设备删除");
        persistence::devices::delete_device(&self.id).await?;
        Ok(())
    }

    async fn connect(conf: &ModbusConf) -> io::Result<Box<dyn Context>> {
        match conf.link_type {
            types::devices::modbus::LinkType::Ethernet => {
                let ethernet = conf.ethernet.as_ref().unwrap();
                let addr: SocketAddr = format!("{}:{}", ethernet.host, ethernet.port)
                    .parse()
                    .unwrap();

                let stream = TcpStream::connect(addr).await?;
                match ethernet.encode {
                    Encode::Tcp => tcp::new(stream),
                    Encode::RtuOverTcp => rtu::new(stream),
                }
            }
            types::devices::modbus::LinkType::Serial => {
                let serial = conf.serial.as_ref().unwrap();
                let builder = tokio_serial::new(serial.path.clone(), serial.baud_rate);
                let mut port = SerialStream::open(&builder).unwrap();
                match serial.stop_bits {
                    types::devices::modbus::StopBits::One => {
                        port.set_stop_bits(StopBits::One).unwrap()
                    }
                    types::devices::modbus::StopBits::Two => {
                        port.set_stop_bits(StopBits::Two).unwrap()
                    }
                };

                match serial.data_bits {
                    types::devices::modbus::DataBits::Five => {
                        port.set_data_bits(DataBits::Five).unwrap()
                    }
                    types::devices::modbus::DataBits::Six => {
                        port.set_data_bits(DataBits::Six).unwrap()
                    }
                    types::devices::modbus::DataBits::Seven => {
                        port.set_data_bits(DataBits::Seven).unwrap()
                    }
                    types::devices::modbus::DataBits::Eight => {
                        port.set_data_bits(DataBits::Eight).unwrap()
                    }
                };

                match serial.parity {
                    types::devices::modbus::Parity::None => port.set_parity(Parity::None).unwrap(),
                    types::devices::modbus::Parity::Odd => port.set_parity(Parity::Odd).unwrap(),
                    types::devices::modbus::Parity::Even => port.set_parity(Parity::Even).unwrap(),
                };

                rtu::new(port)
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
                if self.on {
                    point
                        .start(self.read_tx.as_ref().unwrap().clone(), self.err.clone())
                        .await;
                }
                self.points.write().await.push(point);
                Ok(())
            }
            Err(e) => Err(e),
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
            None => Err(point_not_find_error(point_id)),
        }
    }

    pub async fn write_point_value(&self, point_id: Uuid, value: Value) -> HaliaResult<()> {
        if !self.on {
            return Err(HaliaError::Stopped);
        }

        match self.err.read().await.as_ref() {
            Some(err) => return Err(HaliaError::Common(err.to_string())),
            None => {}
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
                    point.conf.ext.slave,
                    point.conf.ext.area.clone(),
                    point.conf.ext.address,
                    point.conf.ext.data_type.clone(),
                    value.value,
                ) {
                    Ok(wpe) => {
                        match self.write_tx.as_ref().unwrap().send(wpe).await {
                            Ok(_) => trace!("send write value success"),
                            Err(e) => warn!("send write value err:{:?}", e),
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            None => Err(point_not_find_error(point_id)),
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
            None => return Err(point_not_find_error(point_id)),
        }

        self.points
            .write()
            .await
            .retain(|point| point.id != point_id);
        Ok(())
    }

    pub async fn add_point_ref(&mut self, point_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == *point_id)
        {
            Some(point) => Ok(point.add_ref(rule_id)),
            None => Err(point_not_find_error(point_id.clone())),
        }
    }

    pub async fn get_point_mb_rx(
        &mut self,
        point_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        if !self.on {
            return Err(HaliaError::Stopped);
        }
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == *point_id)
        {
            Some(point) => Ok(point.get_mb_rx(rule_id)),
            None => Err(point_not_find_error(point_id.clone())),
        }
    }

    pub async fn del_point_mb_rx(&mut self, point_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == *point_id)
        {
            Some(point) => Ok(point.del_mb_rx(rule_id)),
            None => Err(point_not_find_error(point_id.clone())),
        }
    }

    pub async fn del_point_ref(&mut self, point_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == *point_id)
        {
            Some(point) => Ok(point.del_ref(rule_id)),
            None => Err(point_not_find_error(point_id.clone())),
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
                    sink.start(self.write_tx.as_ref().unwrap().clone(), self.err.clone())
                        .await;
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
            None => Err(sink_not_find_error(sink_id)),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => Err(sink_not_find_error(sink_id)),
        }
    }

    pub fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.add_ref(rule_id)),
            None => Err(sink_not_find_error(sink_id.clone())),
        }
    }

    pub fn get_sink_mb_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.get_mb_tx(rule_id)),
            None => Err(sink_not_find_error(sink_id.clone())),
        }
    }

    pub fn del_sink_mb_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.del_mb_tx(rule_id)),
            None => Err(sink_not_find_error(sink_id.clone())),
        }
    }

    pub fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.del_ref(rule_id)),
            None => Err(sink_not_find_error(sink_id.clone())),
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
                return Err(HaliaError::Common("区域不支持写入操作".to_owned()));
            }
            _ => {}
        }

        let data = match data_type.encode(value) {
            Ok(data) => data,
            Err(e) => {
                return Err(HaliaError::Common(format!("数据解析错误：{:?}", e)));
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

async fn write_value(ctx: &mut Box<dyn Context>, wpe: WritePointEvent) -> HaliaResult<()> {
    let resp = match (wpe.area, wpe.data_type.typ) {
        (Area::Coils, Type::Bool) => {
            ctx.write_single_coil(wpe.slave, wpe.address, wpe.data)
                .await
        }
        (Area::HoldingRegisters, Type::Bool)
        | (Area::HoldingRegisters, Type::Int8)
        | (Area::HoldingRegisters, Type::Uint8) => {
            ctx.mask_write_register(wpe.slave, wpe.address, wpe.data)
                .await
        }
        (Area::HoldingRegisters, Type::Int16) | (Area::HoldingRegisters, Type::Uint16) => {
            ctx.write_single_register(wpe.slave, wpe.address, wpe.data)
                .await
        }
        // 处理特例bytes
        (Area::HoldingRegisters, Type::Int32)
        | (Area::HoldingRegisters, Type::Uint32)
        | (Area::HoldingRegisters, Type::Int64)
        | (Area::HoldingRegisters, Type::Uint64)
        | (Area::HoldingRegisters, Type::Float32)
        | (Area::HoldingRegisters, Type::Float64)
        | (Area::HoldingRegisters, Type::String)
        | (Area::HoldingRegisters, Type::Bytes) => {
            ctx.write_multiple_registers(wpe.slave, wpe.address, wpe.data)
                .await
        }
        _ => unreachable!(),
    };

    match resp {
        Ok(_) => Ok(()),
        Err(e) => match e {
            protocol::modbus::ModbusError::Transport(t) => Err(HaliaError::Io(t)),
            protocol::modbus::ModbusError::Protocol(e) => {
                warn!("modbus protocol err :{:?}", e);
                Ok(())
            }
            protocol::modbus::ModbusError::Exception(e) => {
                warn!("modbus exception err :{:?}", e);
                Ok(())
            }
        },
    }
}
