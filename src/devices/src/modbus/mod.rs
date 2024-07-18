use anyhow::{bail, Result};
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use group::Group;
use message::MessageBatch;
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
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
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
use types::devices::{
    datatype::{DataType, Endian},
    modbus::{
        Area, CreateUpdateGroupPointReq, CreateUpdateGroupReq, CreateUpdateModbusReq,
        CreateUpdateSinkPointReq, CreateUpdateSinkReq, Encode, SearchGroupPointsResp,
        SearchGroupsResp, SearchSinkPointsResp, SearchSinksResp,
    },
    SearchDevicesItemResp,
};
use uuid::Uuid;

pub const TYPE: &str = "modbus";
mod group;
mod group_point;
pub mod manager;
mod sink;
mod sink_point;

#[derive(Debug)]
pub struct Modbus {
    pub id: Uuid,
    err: Arc<AtomicBool>, // true:错误 false:正常

    stop_signal_tx: Option<mpsc::Sender<()>>,

    rtt: Arc<AtomicU16>,
    conf: CreateUpdateModbusReq,
    write_tx: Option<mpsc::Sender<WritePointEvent>>,

    read_tx: Option<mpsc::Sender<Uuid>>,

    groups: Arc<RwLock<Vec<Group>>>,
    sinks: Vec<Sink>,

    handle: Option<
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
            err: Arc::new(AtomicBool::new(false)),
            rtt: Arc::new(AtomicU16::new(9999)),
            conf: req,
            groups: Arc::new(RwLock::new(vec![])),
            read_tx: None,
            write_tx: None,
            sinks: vec![],
            stop_signal_tx: None,
            handle: None,
        })
    }

    pub async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::devices::modbus::read_groups(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);

                    let group_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdateGroupReq = serde_json::from_str(items[1])?;
                    self.create_group(Some(group_id), req).await?;
                }

                for group in self.groups.write().await.iter_mut() {
                    group.recover(&self.id).await?;
                }
            }
            Err(e) => return Err(e.into()),
        }

        match persistence::devices::modbus::read_sinks(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);

                    let sink_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdateSinkReq = serde_json::from_str(items[1])?;
                    self.create_sink(Some(sink_id), req).await?;
                }

                for sink in self.sinks.iter_mut() {
                    sink.recover(&self.id).await?;
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
            err: self.err.load(Ordering::SeqCst),
            conf: json!(&self.conf),
        }
    }

    pub async fn update(&mut self, req: CreateUpdateModbusReq) -> HaliaResult<()> {
        persistence::devices::update_device_conf(&self.id, serde_json::to_string(&req).unwrap())
            .await?;

        let mut restart = false;
        match (&self.conf.link_type, &req.link_type) {
            (
                types::devices::modbus::LinkType::Ethernet,
                types::devices::modbus::LinkType::Ethernet,
            ) => {
                if self.conf.mode != req.mode
                    || self.conf.encode != req.encode
                    || self.conf.host != req.host
                    || self.conf.port != req.port
                {
                    restart = true;
                }
            }
            (
                types::devices::modbus::LinkType::Serial,
                types::devices::modbus::LinkType::Serial,
            ) => {
                if self.conf.path != req.path
                    || self.conf.stop_bits != req.stop_bits
                    || self.conf.baud_rate != req.baud_rate
                    || self.conf.data_bits != req.data_bits
                    || self.conf.parity != req.parity
                {
                    restart = true;
                }
            }
            _ => restart = true,
        }

        self.conf = req;
        if restart && self.stop_signal_tx.is_some() {
            self.restart().await;
        }

        Ok(())
    }

    pub async fn start(&mut self) -> HaliaResult<()> {
        if self.stop_signal_tx.is_some() {
            return Ok(());
        }
        debug!("设备开启");

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);
        let (read_tx, mut read_rx) = mpsc::channel::<Uuid>(16);
        self.read_tx = Some(read_tx);
        let (write_tx, mut write_rx) = mpsc::channel::<WritePointEvent>(16);
        self.write_tx = Some(write_tx);

        let conf = self.conf.clone();
        let groups = self.groups.clone();
        let err = self.err.clone();
        let rtt = self.rtt.clone();

        let handle = tokio::spawn(async move {
            loop {
                match Modbus::get_context(&conf).await {
                    Ok((mut ctx, interval)) => {
                        err.store(false, Ordering::SeqCst);
                        loop {
                            select! {
                                biased;
                                _ = stop_signal_rx.recv() => {
                                    return (stop_signal_rx, write_rx, read_rx);
                                }

                                wpe = write_rx.recv() => {
                                    if let Some(wpe) = wpe {
                                        match write_value(&mut ctx, wpe).await {
                                            Ok(_) => {}
                                            // TODO 识别连接断开
                                            Err(_) => {}
                                        }
                                    }
                                    if interval > 0 {
                                        time::sleep(Duration::from_millis(interval)).await;
                                    }
                                }

                                group_id = read_rx.recv() => {
                                   if let Some(group_id) = group_id {
                                        if let Err(_) = read_group_points(&mut ctx, &groups, group_id, interval, &rtt).await {
                                           err.store(true, Ordering::SeqCst);
                                           break
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => debug!("modbus尝试连接失败"),
                }

                // TODO 重连时间配置化
                time::sleep(Duration::from_secs(30)).await;
            }
        });

        self.handle = Some(handle);

        for group in self.groups.write().await.iter_mut() {
            let read_tx = self.read_tx.as_ref().unwrap().clone();
            group.start(read_tx, self.err.clone());
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if self.stop_signal_tx.is_none() {
            return Ok(());
        }
        debug!("设备停止");

        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        for group in self.groups.write().await.iter_mut() {
            group.stop().await;
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
        self.handle = None;

        Ok(())
    }

    async fn restart(&mut self) {
        let (mut stop_signal_rx, mut write_rx, mut read_rx) =
            self.handle.take().unwrap().await.unwrap();

        // TODO
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if self.stop_signal_tx.is_some() {
            return Err(HaliaError::DeviceRunning);
        }
        debug!("设备删除");
        persistence::devices::delete_device(&self.id).await?;
        Ok(())
    }

    async fn get_context(conf: &CreateUpdateModbusReq) -> HaliaResult<(Context, u64)> {
        match conf.link_type {
            types::devices::modbus::LinkType::Ethernet => {
                let socket_addr: SocketAddr =
                    format!("{}:{}", conf.host.as_ref().unwrap(), conf.port.unwrap())
                        .parse()
                        .unwrap();
                let transport = TcpStream::connect(socket_addr).await?;
                match conf.encode.as_ref().unwrap() {
                    Encode::Tcp => Ok((tcp::attach(transport), conf.interval)),
                    Encode::RtuOverTcp => Ok((rtu::attach(transport), conf.interval)),
                }
            }
            types::devices::modbus::LinkType::Serial => {
                let builder =
                    tokio_serial::new(conf.path.as_ref().unwrap().clone(), conf.baud_rate.unwrap());
                let mut port = SerialStream::open(&builder).unwrap();
                match conf.stop_bits.unwrap() {
                    1 => port.set_stop_bits(StopBits::One).unwrap(),
                    2 => port.set_stop_bits(StopBits::Two).unwrap(),
                    _ => unreachable!(),
                };
                match conf.data_bits.unwrap() {
                    5 => port.set_data_bits(DataBits::Five).unwrap(),
                    6 => port.set_data_bits(DataBits::Six).unwrap(),
                    7 => port.set_data_bits(DataBits::Seven).unwrap(),
                    8 => port.set_data_bits(DataBits::Eight).unwrap(),
                    _ => unreachable!(),
                };

                match conf.parity.unwrap() {
                    0 => port.set_parity(Parity::None).unwrap(),
                    1 => port.set_parity(Parity::Odd).unwrap(),
                    2 => port.set_parity(Parity::Even).unwrap(),
                    _ => unreachable!(),
                };

                Ok((rtu::attach(port), conf.interval))
            }
        }
    }

    async fn get_write_point_event(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        value: String,
    ) -> HaliaResult<WritePointEvent> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.get_write_point_event(point_id, value).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_group(
        &mut self,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match Group::new(&self.id, group_id, req).await {
            Ok(mut group) => {
                if self.stop_signal_tx.is_some() {
                    let read_tx = self.read_tx.as_ref().unwrap().clone();
                    group.start(read_tx, self.err.clone());
                }
                self.groups.write().await.push(group);

                Ok(())
            }
            Err(e) => {
                debug!("{}", e);
                return Err(HaliaError::ConfErr);
            }
        }
    }

    pub async fn search_groups(&self, page: usize, size: usize) -> HaliaResult<SearchGroupsResp> {
        let mut resps = Vec::new();
        for group in self
            .groups
            .read()
            .await
            .iter()
            .rev()
            .skip(((page - 1) * size) as usize)
        {
            resps.push(group.search());
            if resps.len() == size as usize {
                break;
            }
        }
        Ok(SearchGroupsResp {
            total: self.groups.read().await.len(),
            data: resps,
        })
    }

    pub async fn update_group(
        &mut self,
        group_id: Uuid,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => match group.update(&self.id, req).await {
                Ok(restart) => {
                    if restart && self.stop_signal_tx.is_some() {
                        group.stop().await;
                        group.start(self.read_tx.as_ref().unwrap().clone(), self.err.clone());
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            },
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_group(&mut self, group_id: Uuid) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => {
                group.delete(&self.id).await?;
                self.groups
                    .write()
                    .await
                    .retain(|group| group.id != group_id);
            }
            None => return Err(HaliaError::NotFound),
        }

        Ok(())
    }

    pub async fn create_group_point(
        &mut self,
        group_id: Uuid,
        point_id: Option<Uuid>,
        req: CreateUpdateGroupPointReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.create_point(&self.id, point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_group_points(
        &self,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupPointsResp> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => Ok(group.search_points(page, size)),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group_point(
        &mut self,
        group_id: Uuid,
        point_id: Uuid,
        req: CreateUpdateGroupPointReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update_point(&self.id, point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn write_point_value(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        value: String,
    ) -> HaliaResult<()> {
        if self.stop_signal_tx.is_none() {
            return Err(HaliaError::DeviceStoped);
        }
        if self.err.load(Ordering::SeqCst) == true {
            return Err(HaliaError::DeviceDisconnect);
        }

        match self.get_write_point_event(group_id, point_id, value).await {
            Ok(wpe) => {
                let _ = self.write_tx.as_ref().unwrap().send(wpe).await;
                Ok(())
            }
            Err(e) => return Err(e),
        }
    }

    pub async fn delete_group_points(
        &mut self,
        group_id: Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.delete_points(&self.id, point_ids).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn subscribe(
        &mut self,
        group_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => Ok(group.subscribe()),
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn unsubscribe(&mut self, group_id: &Uuid) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => Ok(group.unsubscribe()),
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(&self.id, sink_id, req).await {
            Ok(sink) => Ok(self.sinks.push(sink)),
            Err(e) => Err(e),
        }
    }

    pub async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp {
        let mut data = vec![];
        let mut i = 0;
        for sink in self.sinks.iter().rev().skip((page - 1) * size) {
            data.push(sink.search());
            i += 1;
            if i == size {
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

    pub async fn create_sink_point(
        &mut self,
        sink_id: Uuid,
        point_id: Option<Uuid>,
        req: CreateUpdateSinkPointReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.create_point(&self.id, point_id, req).await?;
                if sink.stop_signal_tx.is_some() {
                    sink.stop().await;
                    sink.start(self.write_tx.as_ref().unwrap().clone());
                }
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_sink_points(
        &self,
        sink_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSinkPointsResp> {
        match self.sinks.iter().find(|sink| sink.id == sink_id) {
            Some(sink) => Ok(sink.search_points(page, size).await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_sink_point(
        &mut self,
        sink_id: Uuid,
        point_id: Uuid,
        req: CreateUpdateSinkPointReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => match sink.update_point(point_id, req).await {
                Ok(restart) => {
                    if restart && self.stop_signal_tx.is_some() {
                        sink.stop().await;
                        sink.start(self.write_tx.as_ref().unwrap().clone());
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            },
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_sink_points(
        &mut self,
        sink_id: Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => match sink.delete_points(&self.id, point_ids).await {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            },
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn publish(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => {
                if self.stop_signal_tx.is_some() && sink.stop_signal_tx.is_none() {
                    sink.start(self.write_tx.as_ref().unwrap().clone());
                }
                sink.publish()
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn unpublish(&mut self, sink_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.unpublish().await),
            None => Err(HaliaError::NotFound),
        }
    }
}

async fn read_group_points(
    ctx: &mut Context,
    groups: &Arc<RwLock<Vec<Group>>>,
    group_id: Uuid,
    interval: u64,
    rtt: &Arc<AtomicU16>,
) -> Result<()> {
    if let Some(group) = groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
    {
        if let Err(e) = group.read_points_value(ctx, interval, rtt).await {
            return Err(e);
        }
    }

    Ok(())
}

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

async fn write_value(ctx: &mut Context, wpe: WritePointEvent) -> Result<()> {
    ctx.set_slave(wpe.slave);
    match wpe.area {
        Area::Coils => match wpe.data_type {
            DataType::Bool(_) => match ctx.write_single_coil(wpe.address, wpe.data[0]).await {
                Ok(res) => match res {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        warn!("modbus protocl exception:{}", e);
                        return Ok(());
                    }
                },
                Err(e) => return Err(e.into()),
            },
            _ => bail!("not support"),
        },
        Area::HoldingRegisters => match wpe.data_type {
            DataType::Bool(pos) => {
                let and_mask = !(1 << pos.unwrap());
                let or_mask = (wpe.data[0] as u16) << pos.unwrap();
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
                    Err(e) => bail!("{}", e),
                }
            }
            DataType::Int8(endian) | DataType::Uint8(endian) => {
                let (and_mask, or_mask) = match endian {
                    Endian::LittleEndian => (0x00FF, (wpe.data[0] as u16) << 8),
                    Endian::BigEndian => (0xFF00, wpe.data[0] as u16),
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
                    Err(e) => bail!("{}", e),
                }
            }
            DataType::Int16(_) | DataType::Uint16(_) => {
                match ctx.write_single_register(wpe.address, &wpe.data).await {
                    Ok(res) => match res {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            warn!("modbus protocol exception:{}", e);
                            return Ok(());
                        }
                    },
                    Err(e) => bail!("{}", e),
                }
            }
            DataType::Int32(_, _)
            | DataType::Uint32(_, _)
            | DataType::Int64(_, _)
            | DataType::Uint64(_, _)
            | DataType::Float32(_, _)
            | DataType::Float64(_, _)
            | DataType::String(_, _, _)
            | DataType::Bytes(_, _, _) => {
                match ctx.write_multiple_registers(wpe.address, &wpe.data).await {
                    Ok(res) => match res {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            warn!("modbus protocol exception:{}", e);
                            return Ok(());
                        }
                    },
                    Err(e) => bail!("{}", e),
                }
            }
        },
        _ => bail!("not support area"),
    }
}
