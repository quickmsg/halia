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

use async_trait::async_trait;
use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id,
    persistence::{self, Status},
};
use message::MessageBatch;
use protocol::modbus::{rtu, tcp, Context};
use sink::Sink;
use source::Source;
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
            Area, CreateUpdatePointReq, CreateUpdateSinkReq, DataType, Encode, ModbusConf,
            PointsQueryParams, SearchPointsResp, SearchSinksResp, SinksQueryParams, Type,
        },
        CreateUpdateDeviceReq, DeviceType, QueryParams, SearchDevicesItemConf,
        SearchDevicesItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp, Value,
};
use uuid::Uuid;

use crate::{sink_not_found_err, source_not_found_err, Device};

// pub mod manager;
mod sink;
mod source;

#[derive(Debug)]
struct Modbus {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: ModbusConf,

    sources: Arc<RwLock<Vec<Source>>>,
    sinks: Vec<Sink>,

    on: bool,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    err: Arc<RwLock<Option<String>>>,
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

pub async fn new(
    device_id: Option<Uuid>,
    req: CreateUpdateDeviceReq,
) -> HaliaResult<Box<dyn Device>> {
    let (base_conf, ext_conf, data) = Modbus::parse_conf(req)?;

    let (device_id, new) = get_id(device_id);
    if new {
        persistence::devices::modbus::create(&device_id, &data).await?;
    }

    Ok(Box::new(Modbus {
        id: device_id,
        base_conf,
        ext_conf,
        on: false,
        err: Arc::new(RwLock::new(None)),
        rtt: Arc::new(AtomicU16::new(9999)),
        sources: Arc::new(RwLock::new(vec![])),
        sinks: vec![],
        read_tx: None,
        write_tx: None,
        stop_signal_tx: None,
        join_handle: None,
    }))
}

impl Modbus {
    fn parse_conf(req: CreateUpdateDeviceReq) -> HaliaResult<(BaseConf, ModbusConf, String)> {
        let data = serde_json::to_string(&req)?;

        let conf: ModbusConf = serde_json::from_value(req.conf.ext)?;

        if conf.ethernet.is_none() && conf.serial.is_none() {
            return Err(HaliaError::Common(
                "必须提供以太网或串口的配置！".to_owned(),
            ));
        }

        // TODO 判断host是否合法

        Ok((req.conf.base, conf, data))
    }

    // pub fn check_duplicate(&self, req: &CreateUpdateModbusReq) -> HaliaResult<()> {
    //     if self.base_c.name == req.base.name {
    //         return Err(HaliaError::Common(format!("名称{}已存在！", req.base.name)));
    //     }

    //     match (&self.conf.ext.ethernet, &req.ext.ethernet) {
    //         (Some(eth_conf), Some(eth_req)) => {
    //             if eth_conf.host == eth_req.host && eth_conf.port == eth_req.port {
    //                 return Err(HaliaError::Common(format!("地址已存在").to_owned()));
    //             }
    //         }
    //         _ => {}
    //     }

    //     match (&self.conf.ext.serial, &req.ext.serial) {
    //         (Some(serial_conf), Some(serial_req)) => {
    //             if serial_conf == serial_req {
    //                 return Err(HaliaError::Common(format!("地址已存在")));
    //             }
    //         }
    //         _ => {}
    //     }

    //     Ok(())
    // }

    // // 重复时返回true
    // pub fn check_duplicate_name(&self, device_id: &Option<Uuid>, name: &str) -> bool {
    //     if let Some(device_id) = device_id {
    //         if *device_id == self.id {
    //             return false;
    //         }
    //     }

    //     self.conf.base.name == name
    // }

    pub async fn recover(&mut self) -> HaliaResult<()> {
        // match persistence::devices::modbus::read_points(&self.id).await {
        //     Ok(datas) => {
        //         for data in datas {
        //             if data.len() == 0 {
        //                 continue;
        //             }
        //             let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
        //             assert_eq!(items.len(), 2);

        //             let point_id = Uuid::from_str(items[0]).unwrap();
        //             let req: CreateUpdatePointReq = serde_json::from_str(items[1])?;
        //             self.create_point(Some(point_id), req).await?;
        //         }
        //     }
        //     Err(e) => return Err(e.into()),
        // }

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

    pub async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);
        trace!("设备开启");

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (read_tx, read_rx) = mpsc::channel::<Uuid>(16);
        let (write_tx, write_rx) = mpsc::channel::<WritePointEvent>(16);

        for source in self.sources.write().await.iter_mut() {
            source.start(read_tx.clone(), self.err.clone()).await;
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
        let modbus_conf = self.ext_conf.clone();
        let interval = modbus_conf.interval;
        let sources = self.sources.clone();
        let reconnect = modbus_conf.reconnect;

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
                                        match sources.write().await.iter_mut().find(|point| point.id == point_id) {
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
        if self
            .sources
            .read()
            .await
            .iter()
            .any(|point| !point.ref_info.can_stop())
        {
            return Err(HaliaError::Common("设备有源被运行规则引用中".to_owned()));
        }

        if self.sinks.iter().any(|sink| !sink.ref_info.can_stop()) {
            return Err(HaliaError::Common("设备有动作被运行规则引用中".to_owned()));
        }

        check_and_set_on_false!(self);
        trace!("停止");

        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        for point in self.sources.write().await.iter_mut() {
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

    // pub async fn create_point(
    //     &mut self,
    //     point_id: Option<Uuid>,
    //     req: CreateUpdatePointReq,
    // ) -> HaliaResult<()> {
    //     for point in self.points.read().await.iter() {
    //         point.check_duplicate(&req)?;
    //     }
    //     match Point::new(&self.id, point_id, req).await {
    //         Ok(mut point) => {
    //             if self.on {
    //                 point
    //                     .start(self.read_tx.as_ref().unwrap().clone(), self.err.clone())
    //                     .await;
    //             }
    //             self.points.write().await.push(point);
    //             Ok(())
    //         }
    //         Err(e) => Err(e),
    //     }
    // }

    pub async fn search_points(
        &self,
        pagination: Pagination,
        query_params: PointsQueryParams,
    ) -> SearchPointsResp {
        let mut total = 0;
        let mut data = vec![];
        for point in self.points.read().await.iter().rev() {
            let point = point.search();
            if let Some(query_name) = &query_params.name {
                if !point.conf.base.name.contains(query_name) {
                    continue;
                }
            }

            if total >= (pagination.page - 1) * pagination.size
                && total < pagination.page * pagination.size
            {
                data.push(point);
            }

            total += 1;
        }

        SearchPointsResp { total, data }
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
            None => source_not_found_err!(),
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
            .sources
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
            None => source_not_found_err!(),
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
            None => return source_not_found_err!(),
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
            Some(point) => Ok(point.ref_info.add_ref(rule_id)),
            None => source_not_found_err!(),
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
            None => source_not_found_err!(),
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
            None => source_not_found_err!(),
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
            Some(point) => Ok(point.ref_info.del_ref(rule_id)),
            None => source_not_found_err!(),
        }
    }

    pub async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        for sink in &self.sinks {
            sink.check_duplicate(&req)?;
        }

        match Sink::new(&self.id, sink_id, req).await {
            Ok(mut sink) => {
                if self.on {
                    sink.start(self.write_tx.as_ref().unwrap().clone(), self.err.clone())
                        .await;
                }
                self.sinks.push(sink);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn search_sinks(
        &self,
        pagination: Pagination,
        query_params: SinksQueryParams,
    ) -> SearchSinksResp {
        let mut total = 0;
        let mut data = vec![];
        for sink in self.sinks.iter().rev() {
            let sink = sink.search();

            if let Some(query_name) = &query_params.name {
                if !sink.conf.base.name.contains(query_name) {
                    continue;
                }
            }

            if total >= (pagination.page - 1) * pagination.size
                && total < pagination.page * pagination.size
            {
                data.push(sink);
            }

            total += 1;
        }

        SearchSinksResp { total, data }
    }

    pub async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.update(&self.id, req).await,
            None => sink_not_found_err!(),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => sink_not_found_err!(),
        }
    }

    pub fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.add_ref(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    pub fn get_sink_mb_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.get_mb_tx(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    pub fn del_sink_mb_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.deactive_ref(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    pub fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.del_ref(rule_id)),
            None => sink_not_found_err!(),
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
        (Area::HoldingRegisters, Type::Int32)
        | (Area::HoldingRegisters, Type::Uint32)
        | (Area::HoldingRegisters, Type::Int64)
        | (Area::HoldingRegisters, Type::Uint64)
        | (Area::HoldingRegisters, Type::Float32)
        | (Area::HoldingRegisters, Type::Float64) => {
            ctx.write_multiple_registers(wpe.slave, wpe.address, wpe.data)
                .await
        }

        (Area::HoldingRegisters, Type::String) | (Area::HoldingRegisters, Type::Bytes) => {
            let len = wpe.data_type.len.as_ref().unwrap();
            if *len == 1 {
                ctx.write_single_register(wpe.slave, wpe.address, wpe.data)
                    .await
            } else {
                ctx.write_multiple_registers(wpe.slave, wpe.address, wpe.data)
                    .await
            }
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

#[async_trait]
impl Device for Modbus {
    fn get_id(&self) -> Uuid {
        self.id.clone()
    }

    async fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id.clone(),
            typ: DeviceType::Modbus,
            rtt: self.rtt.load(Ordering::SeqCst),
            on: self.on,
            err: self.err.read().await.clone(),
            conf: SearchDevicesItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::json!(self.ext_conf),
            },
        }
    }

    async fn update(&mut self, req: CreateUpdateDeviceReq) -> HaliaResult<()> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;

        persistence::devices::update_device_conf(&self.id, data).await?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

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

    async fn delete(&mut self) -> HaliaResult<()> {
        if self.on {
            return Err(HaliaError::Running);
        }

        if self
            .sources
            .read()
            .await
            .iter()
            .any(|point| point.ref_info.can_delete())
        {
            return Err(HaliaError::Common("设备点位被规则引用中".to_owned()));
        }
        if self.sinks.iter().any(|sink| sink.ref_info.can_delete()) {
            return Err(HaliaError::Common("设备动作被规则引用中".to_owned()));
        }

        trace!("设备删除");
        persistence::devices::delete_device(&self.id).await?;

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        // for point in self.points.read().await.iter() {
        //     point.check_duplicate(&req)?;
        // }
        match Source::new(&self.id, source_id, req).await {
            Ok(mut source) => {
                if self.on {
                    source
                        .start(self.read_tx.as_ref().unwrap().clone(), self.err.clone())
                        .await;
                }
                self.sources.write().await.push(source);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sources(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> HaliaResult<SearchSourcesOrSinksResp> {
        let mut total = 0;
        let mut data = vec![];
        for source in self.sources.read().await.iter().rev() {
            let source = source.search();
            if let Some(name) = &query.name {
                if !source.conf.base.name.contains(name) {
                    continue;
                }
            }

            if total >= (pagination.page - 1) * pagination.size
                && total < pagination.page * pagination.size
            {
                data.push(source);
            }

            total += 1;
        }

        SearchSourcesOrSinksResp {total,data, rule_ref: todo!() } }
    }

    async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn create_sink<'life0, 'async_trait>(
        &'life0 mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn search_sinks<'life0, 'async_trait>(
        &'life0 self,
        pagination: Pagination,
        query: QueryParams,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<SearchSourcesOrSinksResp>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn update_sink<'life0, 'async_trait>(
        &'life0 mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn delete_sink<'life0, 'async_trait>(
        &'life0 mut self,
        sink_id: Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    async fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        todo!()
    }

    async fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn add_sink_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        todo!()
    }

    async fn del_sink_tx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn del_sink_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }
}
