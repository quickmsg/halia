use std::{
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use common::{
    active_sink_ref, active_source_ref, check_and_set_on_false, check_and_set_on_true,
    check_delete_item, deactive_sink_ref, deactive_source_ref, del_sink_ref, del_source_ref,
    error::{HaliaError, HaliaResult},
    find_sink_add_ref, find_source_add_ref,
    ref_info::RefInfo,
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
        modbus::{Area, DataType, Encode, ModbusConf, SinkConf, SourceConf, Type},
        CreateUpdateDeviceReq, DeviceConf, DeviceType, QueryParams, SearchDevicesItemCommon,
        SearchDevicesItemConf, SearchDevicesItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksItemResp,
    SearchSourcesOrSinksResp, Value,
};
use uuid::Uuid;

use crate::{sink_not_found_err, source_not_found_err, Device};

mod sink;
mod source;

#[derive(Debug)]
struct Modbus {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: ModbusConf,

    sources: Arc<RwLock<Vec<Source>>>,
    sources_ref_infos: Vec<(Uuid, RefInfo)>,
    sinks: Vec<Sink>,
    sinks_ref_infos: Vec<(Uuid, RefInfo)>,

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

pub fn new(device_id: Uuid, device_conf: DeviceConf) -> HaliaResult<Box<dyn Device>> {
    let ext_conf: ModbusConf = serde_json::from_value(device_conf.ext)?;
    Modbus::validate_conf(&ext_conf)?;

    Ok(Box::new(Modbus {
        id: device_id,
        base_conf: device_conf.base,
        ext_conf,
        on: false,
        err: Arc::new(RwLock::new(None)),
        rtt: Arc::new(AtomicU16::new(9999)),
        sources: Arc::new(RwLock::new(vec![])),
        sources_ref_infos: vec![],
        sinks: vec![],
        sinks_ref_infos: vec![],
        read_tx: None,
        write_tx: None,
        stop_signal_tx: None,
        join_handle: None,
    }))
}

impl Modbus {
    fn validate_conf(conf: &ModbusConf) -> HaliaResult<()> {
        if conf.ethernet.is_none() && conf.serial.is_none() {
            return Err(HaliaError::Common(
                "必须提供以太网或串口的配置！".to_owned(),
            ));
        }

        Ok(())
    }

    fn check_on(&self) -> HaliaResult<()> {
        match self.on {
            true => Ok(()),
            false => Err(HaliaError::Stopped(format!(
                "modbus设备:{}",
                self.base_conf.name
            ))),
        }
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
                                        match sources.write().await.iter_mut().find(|source| source.id == point_id) {
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
                return Err(HaliaError::Common("区域不支持写入操作!".to_owned()));
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
    fn get_id(&self) -> &Uuid {
        &self.id
    }

    fn check_duplicate(&self, req: &CreateUpdateDeviceReq) -> HaliaResult<()> {
        if self.base_conf.name == req.conf.base.name {
            return Err(HaliaError::NameExists);
        }

        if req.device_type == DeviceType::Modbus {
            let conf: ModbusConf = serde_json::from_value(req.conf.ext.clone())?;

            match (&self.ext_conf.ethernet, &conf.ethernet) {
                (Some(eth_conf), Some(eth_req)) => {
                    if eth_conf.host == eth_req.host && eth_conf.port == eth_req.port {
                        return Err(HaliaError::Common(format!("地址已存在").to_owned()));
                    }
                }
                _ => {}
            }

            match (&self.ext_conf.serial, &conf.serial) {
                (Some(serial_conf), Some(serial_req)) => {
                    if serial_conf == serial_req {
                        return Err(HaliaError::Common(format!("地址已存在")));
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            common: SearchDevicesItemCommon {
                id: self.id.clone(),
                device_type: DeviceType::Modbus,
                rtt: self.rtt.load(Ordering::SeqCst),
                on: self.on,
                err: self.err.read().await.clone(),
            },
            conf: SearchDevicesItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::json!(self.ext_conf),
            },
        }
    }

    async fn update(&mut self, device_conf: DeviceConf) -> HaliaResult<()> {
        let ext_conf: ModbusConf = serde_json::from_value(device_conf.ext)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = device_conf.base;
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

    async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);
        trace!("设备开启");

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

    async fn stop(&mut self) -> HaliaResult<()> {
        if self
            .sources_ref_infos
            .iter()
            .any(|(_, ref_info)| !ref_info.can_stop())
        {
            return Err(HaliaError::StopActiveRefing);
        }

        if self
            .sinks_ref_infos
            .iter()
            .any(|(_, ref_info)| !ref_info.can_stop())
        {
            return Err(HaliaError::StopActiveRefing);
        }

        check_and_set_on_false!(self);
        trace!("停止");

        for source in self.sources.write().await.iter_mut() {
            source.stop().await;
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

    async fn delete(&mut self) -> HaliaResult<()> {
        if self.on {
            return Err(HaliaError::Running);
        }

        if self
            .sources_ref_infos
            .iter()
            .any(|(_, ref_info)| ref_info.can_delete())
        {
            return Err(HaliaError::DeleteRefing);
        }
        if self
            .sinks_ref_infos
            .iter()
            .any(|(_, ref_info)| ref_info.can_delete())
        {
            return Err(HaliaError::DeleteRefing);
        }

        trace!("设备删除");

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        Source::validate_conf(&ext_conf)?;

        for source in self.sources.read().await.iter() {
            source.check_duplicate(&req.base, &ext_conf)?;
        }

        let mut source = Source::new(source_id, req.base, ext_conf);
        if self.on {
            source
                .start(self.read_tx.as_ref().unwrap().clone(), self.err.clone())
                .await;
        }

        self.sources.write().await.push(source);
        self.sources_ref_infos.push((source_id, RefInfo::new()));
        Ok(())
    }

    async fn search_sources(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];
        for (index, source) in self.sources.read().await.iter().rev().enumerate() {
            let source = source.search();
            if let Some(name) = &query.name {
                if !source.conf.base.name.contains(name) {
                    continue;
                }
            }

            if pagination.check(total) {
                unsafe {
                    data.push(SearchSourcesOrSinksItemResp {
                        info: source,
                        rule_ref: self.sources_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    });
                }
            }

            total += 1;
        }

        SearchSourcesOrSinksResp { total, data }
    }

    async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        Source::validate_conf(&ext_conf)?;

        for source in self.sources.read().await.iter() {
            if source.id != source_id {
                source.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => Ok(source.update(req.base, ext_conf).await),
            None => source_not_found_err!(),
        }
    }

    async fn write_source_value(&mut self, source_id: Uuid, req: Value) -> HaliaResult<()> {
        self.check_on()?;

        match self.err.read().await.as_ref() {
            Some(err) => return Err(HaliaError::Common(err.to_string())),
            None => {}
        }

        match self
            .sources
            .read()
            .await
            .iter()
            .find(|source| source.id == source_id)
        {
            Some(source) => {
                match WritePointEvent::new(
                    source.ext_conf.slave,
                    source.ext_conf.area.clone(),
                    source.ext_conf.address,
                    source.ext_conf.data_type.clone(),
                    req.value,
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

    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
        check_delete_item!(self, sources_ref_infos, source_id);

        if self.on {
            match self
                .sources
                .write()
                .await
                .iter_mut()
                .find(|source| source.id == source_id)
            {
                Some(source) => source.stop().await,
                None => unreachable!(),
            }
        }

        self.sources
            .write()
            .await
            .retain(|source| source.id != source_id);
        self.sources_ref_infos.retain(|(id, _)| *id != source_id);
        Ok(())
    }

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        Sink::validate_conf(&ext_conf)?;

        for sink in self.sinks.iter() {
            sink.check_duplicate(&req.base, &ext_conf)?;
        }

        let mut sink = Sink::new(sink_id, req.base, ext_conf);
        if self.on {
            sink.start(self.write_tx.as_ref().unwrap().clone(), self.err.clone())
                .await;
        }

        self.sinks.push(sink);
        self.sinks_ref_infos.push((sink_id, RefInfo::new()));

        Ok(())
    }

    async fn search_sinks(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];
        for (index, sink) in self.sinks.iter().rev().enumerate() {
            let sink = sink.search();

            if let Some(name) = &query.name {
                if !sink.conf.base.name.contains(name) {
                    continue;
                }
            }

            if pagination.check(total) {
                unsafe {
                    data.push(SearchSourcesOrSinksItemResp {
                        info: sink,
                        rule_ref: self.sinks_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    })
                }
            }

            total += 1;
        }

        SearchSourcesOrSinksResp { total, data }
    }

    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        Sink::validate_conf(&ext_conf)?;

        for sink in self.sinks.iter() {
            if sink.id != sink_id {
                sink.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => Ok(sink.update(req.base, ext_conf).await),
            None => sink_not_found_err!(),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        check_delete_item!(self, sinks_ref_infos, sink_id);
        if self.on {
            match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
                Some(sink) => sink.stop().await,
                None => unreachable!(),
            }
        }

        self.sinks.retain(|sink| sink.id != sink_id);
        self.sinks_ref_infos.retain(|(id, _)| *id != sink_id);
        Ok(())
    }

    async fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        find_source_add_ref!(self, source_id, rule_id)
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        self.check_on()?;
        active_source_ref!(self, source_id, rule_id);
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.mb_tx.as_ref().unwrap().subscribe()),
            None => unreachable!(),
        }
    }

    async fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        deactive_source_ref!(self, source_id, rule_id)
    }

    async fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_source_ref!(self, source_id, rule_id)
    }

    async fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        find_sink_add_ref!(self, sink_id, rule_id)
    }

    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        self.check_on()?;
        active_sink_ref!(self, sink_id, rule_id);
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.mb_tx.as_ref().unwrap().clone()),
            None => unreachable!(),
        }
    }

    async fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        deactive_sink_ref!(self, sink_id, rule_id)
    }

    async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_sink_ref!(self, sink_id, rule_id)
    }
}
