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
    constants::CHANNEL_SIZE,
    error::{HaliaError, HaliaResult},
};
use dashmap::DashMap;
use message::MessageBatch;
use protocol::modbus::{self, rtu, tcp, Context};
use sink::Sink;
use source::Source;
use tokio::{
    net::{lookup_host, TcpStream},
    select,
    sync::{broadcast, mpsc, watch, RwLock},
    task::JoinHandle,
    time,
};
use tokio_serial::{DataBits, Parity, SerialPort, SerialStream, StopBits};
use tracing::{trace, warn};
use types::{
    devices::{
        device::RunningInfo,
        modbus::{
            Area, Conf, DataType, Encode, SinkConf, SourceConf, SourceCustomizeConf,
            SourceTemplateConf, Type,
        },
    },
    Value,
};

use crate::{add_device_running_count, sub_device_running_count, Device};

mod sink;
pub(crate) mod sink_template;
mod source;
pub(crate) mod source_template;
pub(crate) mod template;

struct Modbus {
    sources: Arc<DashMap<String, Source>>,
    sinks: DashMap<String, Sink>,

    stop_signal_tx: watch::Sender<()>,
    device_err_tx: broadcast::Sender<bool>,
    err: Arc<RwLock<Option<String>>>,
    rtt: Arc<AtomicU16>,

    write_tx: mpsc::Sender<WritePointEvent>,
    read_tx: mpsc::Sender<String>,

    join_handle: Option<JoinHandle<JoinHandleData>>,
}

struct JoinHandleData {
    pub id: String,
    pub conf: Conf,
    pub sources: Arc<DashMap<String, Source>>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub write_rx: mpsc::Receiver<WritePointEvent>,
    pub read_rx: mpsc::Receiver<String>,
    pub err: Arc<RwLock<Option<String>>>,
    pub rtt: Arc<AtomicU16>,
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: Conf = serde_json::from_value(conf.clone())?;

    match conf.link_type {
        types::devices::modbus::LinkType::Ethernet => {
            if conf.ethernet.is_none() {
                return Err(HaliaError::Common("必须提供以太网的配置！".to_owned()));
            }
        }
        types::devices::modbus::LinkType::Serial => {
            if conf.serial.is_none() {
                return Err(HaliaError::Common("必须提供串口的配置！".to_owned()));
            }
        }
    }

    Ok(())
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn Device> {
    let conf: Conf = serde_json::from_value(conf).unwrap();

    let (stop_signal_tx, stop_signal_rx) = watch::channel(());
    let (read_tx, read_rx) = mpsc::channel::<String>(CHANNEL_SIZE);
    let (write_tx, write_rx) = mpsc::channel::<WritePointEvent>(CHANNEL_SIZE);
    let (device_err_tx, _) = broadcast::channel(CHANNEL_SIZE);

    let sources = Arc::new(DashMap::new());
    let err = Arc::new(RwLock::new(None));
    let rtt = Arc::new(AtomicU16::new(0));
    let join_handle_data = JoinHandleData {
        id,
        conf,
        stop_signal_rx,
        write_rx,
        read_rx,
        sources: sources.clone(),
        err: err.clone(),
        rtt: rtt.clone(),
    };

    let join_handle = Modbus::event_loop(join_handle_data);

    Box::new(Modbus {
        err,
        rtt,
        sources,
        sinks: DashMap::new(),
        stop_signal_tx,
        device_err_tx,
        write_tx,
        read_tx,
        join_handle: Some(join_handle),
    })
}

pub fn validate_source_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    // let conf: SourceConf = serde_json::from_value(conf.clone())?;
    // Source::validate_conf(&conf)?;
    Ok(())
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

impl Modbus {
    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            let mut task_err: Option<String> = Some("not connectd.".to_owned());
            loop {
                match Modbus::connect(&join_handle_data.conf).await {
                    Ok(mut ctx) => {
                        add_device_running_count();
                        task_err = None;
                        *join_handle_data.err.write().await = None;
                        if let Err(e) =
                            storage::device::device::update_err(&join_handle_data.id, false).await
                        {
                            warn!("update device err failed: {}", e);
                        }

                        events::insert_connect_succeed(
                            types::events::ResourceType::Device,
                            &join_handle_data.id,
                        )
                        .await;

                        loop {
                            select! {
                                biased;
                                _ = join_handle_data.stop_signal_rx.changed() => {
                                    return join_handle_data;
                                }

                                wpe = join_handle_data.write_rx.recv() => {
                                    if let Some(wpe) = wpe {
                                        if write_value(&mut ctx, wpe).await.is_err() {
                                            break
                                        }
                                    }
                                    if join_handle_data.conf.interval > 0 {
                                        time::sleep(Duration::from_millis(join_handle_data.conf.interval)).await;
                                    }
                                }

                                Some(point_id) = join_handle_data.read_rx.recv() => {
                                    if let Some(mut source) = join_handle_data.sources.get_mut(&point_id) {
                                        let now = Instant::now();
                                        if let Err(_) = source.read(&mut ctx).await {
                                            break
                                        }
                                        join_handle_data.rtt.store(now.elapsed().as_secs() as u16, Ordering::SeqCst);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        events::insert_connect_failed(
                            types::events::ResourceType::Device,
                            &join_handle_data.id,
                            e.to_string(),
                        )
                        .await;

                        match &task_err {
                            Some(te) => {
                                if *te != e.to_string() {
                                    *join_handle_data.err.write().await = Some(e.to_string());
                                }
                            }
                            None => {
                                sub_device_running_count();
                                *join_handle_data.err.write().await = Some(e.to_string());
                                if let Err(storage_err) =
                                    storage::device::device::update_err(&join_handle_data.id, true)
                                        .await
                                {
                                    warn!("update device err failed: {}", storage_err);
                                }
                            }
                        }

                        task_err = Some(e.to_string());

                        let sleep =
                            time::sleep(Duration::from_secs(join_handle_data.conf.reconnect));
                        tokio::pin!(sleep);
                        select! {
                            _ = join_handle_data.stop_signal_rx.changed() => {
                                return join_handle_data;
                            }

                            _ = &mut sleep => {}
                        }
                    }
                }
            }
        })
    }

    async fn connect(conf: &Conf) -> io::Result<Box<dyn Context>> {
        match conf.link_type {
            types::devices::modbus::LinkType::Ethernet => {
                let ethernet = conf.ethernet.as_ref().unwrap();
                let socket_addrs = lookup_host(format!("{}:{}", ethernet.host, ethernet.port))
                    .await?
                    .collect::<Vec<SocketAddr>>();
                if socket_addrs.len() == 0 {
                    return Err(io::Error::new(io::ErrorKind::Other, "no address found"));
                }
                let stream = TcpStream::connect(socket_addrs[0]).await?;

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

async fn write_value(ctx: &mut Box<dyn modbus::Context>, wpe: WritePointEvent) -> HaliaResult<()> {
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
    async fn read_running_info(&self) -> RunningInfo {
        RunningInfo {
            err: self.err.read().await.clone(),
            rtt: self.rtt.load(Ordering::SeqCst),
        }
    }

    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: Conf = serde_json::from_value(new_conf)?;

        self.stop_signal_tx.send(()).unwrap();
        let mut join_handle_data = self.join_handle.take().unwrap().await.unwrap();
        join_handle_data.conf = new_conf;
        let join_handle = Self::event_loop(join_handle_data);
        self.join_handle = Some(join_handle);

        Ok(())
    }

    async fn stop(&mut self) {
        for mut source in self.sources.iter_mut() {
            source.stop().await;
        }

        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        self.stop_signal_tx.send(()).unwrap();
    }

    async fn create_customize_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(
            source_id.clone(),
            conf,
            self.read_tx.clone(),
            self.device_err_tx.subscribe(),
        );
        self.sources.insert(source_id, source);
        Ok(())
    }

    // TODO
    async fn create_template_source(
        &mut self,
        source_id: String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf = get_source_conf(customize_conf, template_conf)?;
        let source = Source::new(
            source_id.clone(),
            conf,
            self.read_tx.clone(),
            self.device_err_tx.subscribe(),
        );
        self.sources.insert(source_id, source);
        Ok(())
    }

    async fn update_customize_source(
        &mut self,
        source_id: &String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        match self.sources.get_mut(source_id) {
            Some(mut source) => {
                source.update(conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn update_template_source(
        &mut self,
        source_id: &String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf = get_source_conf(customize_conf, template_conf)?;
        match self.sources.get_mut(source_id) {
            Some(mut source) => {
                source.update(conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn write_source_value(&mut self, source_id: String, req: Value) -> HaliaResult<()> {
        if let Some(err) = self.err.read().await.as_ref() {
            return Err(HaliaError::Common(err.to_string()));
        }

        match self.sources.get(&source_id) {
            Some(source) => {
                match WritePointEvent::new(
                    source.conf.slave,
                    source.conf.area.clone(),
                    source.conf.address,
                    source.conf.data_type.clone(),
                    req.value,
                ) {
                    Ok(wpe) => {
                        match self.write_tx.send(wpe).await {
                            Ok(_) => trace!("send write value success"),
                            Err(e) => warn!("send write value err:{:?}", e),
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            None => Err(HaliaError::NotFound(source_id)),
        }
    }

    async fn delete_source(&mut self, source_id: &String) -> HaliaResult<()> {
        match self.sources.remove(source_id) {
            Some((_, mut source)) => {
                source.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn create_customize_sink(
        &mut self,
        sink_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(conf, self.write_tx.clone(), self.device_err_tx.subscribe());
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn create_template_sink(
        &mut self,
        sink_id: String,
        conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(conf, self.write_tx.clone(), self.device_err_tx.subscribe());
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let old_conf: SinkConf = serde_json::from_value(old_conf)?;
        let new_conf: SinkConf = serde_json::from_value(new_conf)?;
        match self.sinks.get_mut(sink_id) {
            Some(mut sink) => {
                sink.update(old_conf, new_conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }

    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()> {
        match self.sinks.remove(sink_id) {
            Some((_, mut sink)) => {
                sink.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.sources.get(source_id) {
            Some(source) => Ok(source.mb_tx.subscribe()),
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

fn get_source_conf(
    customize_conf: serde_json::Value,
    template_conf: serde_json::Value,
) -> HaliaResult<SourceConf> {
    let customize_conf: SourceCustomizeConf = serde_json::from_value(customize_conf)?;
    let template_conf: SourceTemplateConf = serde_json::from_value(template_conf)?;
    Ok(SourceConf {
        field: template_conf.field,
        data_type: template_conf.data_type,
        slave: customize_conf.slave,
        area: template_conf.area,
        address: template_conf.address,
        interval: template_conf.interval,
    })
}
