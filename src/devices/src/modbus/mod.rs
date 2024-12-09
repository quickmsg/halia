use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use futures::lock::BiLock;
use halia_derive::ResourceErr;
use message::RuleMessageBatch;
use modbus_protocol::{self, rtu, tcp, Context};
use sink::Sink;
use source::Source;
use tokio::{
    net::{lookup_host, TcpStream},
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
    time,
};
use tokio_serial::{DataBits, Parity, SerialPort, SerialStream, StopBits};
use tracing::{debug, trace, warn};
use types::{
    devices::{
        device::modbus::{
            Area, DataType, DeviceConf, Encode, Ethernet, Serial, SinkConf, SourceConf, Type,
        },
        device_template::modbus::{CustomizeConf, TemplateConf},
        source_sink_template::modbus::{
            SinkCustomizeConf, SinkTemplateConf, SourceCustomizeConf, SourceTemplateConf,
        },
    },
    Value,
};
use utils::ErrorManager;

use crate::Device;

mod sink;
mod source;
pub(crate) mod template;

#[derive(ResourceErr)]
struct Modbus {
    sources: Arc<DashMap<String, Source>>,
    sinks: DashMap<String, Sink>,

    stop_signal_tx: watch::Sender<()>,
    device_err_tx: broadcast::Sender<bool>,
    err: BiLock<Option<Arc<String>>>,

    write_tx: UnboundedSender<WritePointEvent>,
    read_tx: UnboundedSender<String>,

    join_handle: Option<JoinHandle<TaskLoop>>,
}

struct TaskLoop {
    device_conf: DeviceConf,
    error_manager: ErrorManager,
    sources: Arc<DashMap<String, Source>>,
    stop_signal_rx: watch::Receiver<()>,
    write_rx: UnboundedReceiver<WritePointEvent>,
    read_rx: UnboundedReceiver<String>,
}

impl TaskLoop {
    pub fn new(
        device_id: String,
        device_conf: DeviceConf,
        err: BiLock<Option<Arc<String>>>,
        stop_signal_rx: watch::Receiver<()>,
        sources: Arc<DashMap<String, Source>>,
        write_rx: UnboundedReceiver<WritePointEvent>,
        read_rx: UnboundedReceiver<String>,
    ) -> Self {
        let error_manager =
            ErrorManager::new(utils::error_manager::ResourceType::Device, device_id, err);
        Self {
            device_conf,
            error_manager,
            sources,
            stop_signal_rx,
            write_rx,
            read_rx,
        }
    }

    pub fn start(mut self) -> JoinHandle<TaskLoop> {
        tokio::spawn(async move {
            loop {
                match self.connect().await {
                    Ok(mut ctx) => {
                        self.error_manager.set_ok().await;
                        loop {
                            select! {
                                biased;
                                _ = self.stop_signal_rx.changed() => {
                                    return self;
                                }

                                wpe = self.write_rx.recv() => {
                                    if let Some(wpe) = wpe {
                                        if write_value(&mut ctx, wpe).await.is_err() {
                                            break
                                        }
                                    }
                                    if self.device_conf.interval > 0 {
                                        time::sleep(Duration::from_millis(self.device_conf.interval)).await;
                                    }
                                }

                                Some(point_id) = self.read_rx.recv() => {
                                    if let Some(mut source) = self.sources.get_mut(&point_id) {
                                        if let Err(_) = source.read(&mut ctx, &self.device_conf).await {
                                            break
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let e = Arc::new(e.to_string());
                        self.error_manager.set_err(e.clone()).await;
                        let sleep = time::sleep(Duration::from_secs(self.device_conf.reconnect));
                        tokio::pin!(sleep);
                        select! {
                            _ = self.stop_signal_rx.changed() => {
                                return self;
                            }

                            _ = &mut sleep => {}
                        }
                    }
                }
            }
        })
    }

    pub async fn connect(&self) -> io::Result<Box<dyn Context>> {
        match self.device_conf.link_type {
            types::devices::device::modbus::LinkType::Ethernet => {
                let ethernet = self.device_conf.ethernet.as_ref().unwrap();
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
            types::devices::device::modbus::LinkType::Serial => {
                let serial = &self.device_conf.serial.as_ref().unwrap();
                let builder = tokio_serial::new(serial.path.clone(), serial.baud_rate);

                let mut port = SerialStream::open(&builder).unwrap();
                let stop_bits = match serial.stop_bits {
                    types::devices::device::modbus::StopBits::One => StopBits::One,
                    types::devices::device::modbus::StopBits::Two => StopBits::Two,
                };
                port.set_stop_bits(stop_bits).unwrap();

                let data_bits = match serial.data_bits {
                    types::devices::device::modbus::DataBits::Five => DataBits::Five,
                    types::devices::device::modbus::DataBits::Six => DataBits::Six,
                    types::devices::device::modbus::DataBits::Seven => DataBits::Seven,
                    types::devices::device::modbus::DataBits::Eight => DataBits::Eight,
                };
                port.set_data_bits(data_bits).unwrap();

                let partity = match serial.parity {
                    types::devices::device::modbus::Parity::None => Parity::None,
                    types::devices::device::modbus::Parity::Odd => Parity::Odd,
                    types::devices::device::modbus::Parity::Even => Parity::Even,
                };
                port.set_parity(partity).unwrap();

                rtu::new(port)
            }
        }
    }

    pub fn update_template_conf(&mut self, template_conf: serde_json::Value) -> HaliaResult<()> {
        let template_conf: TemplateConf = serde_json::from_value(template_conf)?;
        match template_conf.link_type {
            types::devices::device::modbus::LinkType::Ethernet => {
                match (&self.device_conf.ethernet, template_conf.ethernet) {
                    (Some(ethernet_customize_conf), Some(ethernet_template_conf)) => {
                        // Ok(DeviceConf {
                        //     link_type: template_conf.link_type,
                        //     reconnect: template_conf.reconnect,
                        //     interval: template_conf.interval,
                        //     ethernet: Some(Ethernet {
                        //         mode: ethernet_template_conf.mode,
                        //         encode: ethernet_template_conf.encode,
                        //         host: ethernet_customize_conf.host,
                        //         port: ethernet_customize_conf.port,
                        //     }),
                        //     serial: None,
                        //     metadatas: customize_conf.metadatas,
                        // })
                        self.device_conf.reconnect = template_conf.reconnect;
                        self.device_conf.interval = template_conf.interval;
                        self.device_conf.ethernet.as_mut().unwrap().mode =
                            ethernet_template_conf.mode;
                        self.device_conf.ethernet.as_mut().unwrap().encode =
                            ethernet_template_conf.encode;
                    }
                    _ => unreachable!(),
                }
            }
            types::devices::device::modbus::LinkType::Serial => {
                match (&self.device_conf.serial, template_conf.serial) {
                    (Some(serial_customize_conf), Some(serial_template_conf)) => {
                        self.device_conf.serial.as_mut().unwrap().stop_bits =
                            serial_template_conf.stop_bits;
                        self.device_conf.serial.as_mut().unwrap().baud_rate =
                            serial_template_conf.baud_rate;
                        self.device_conf.serial.as_mut().unwrap().data_bits =
                            serial_template_conf.data_bits;
                        self.device_conf.serial.as_mut().unwrap().parity =
                            serial_template_conf.parity;
                    }
                    // Ok(DeviceConf {
                    //     link_type: template_conf.link_type,
                    //     reconnect: template_conf.reconnect,
                    //     interval: template_conf.interval,
                    //     ethernet: None,
                    //     serial: Some(Serial {
                    //         path: serial_customize_conf.path,
                    //         stop_bits: serial_template_conf.stop_bits,
                    //         baud_rate: serial_template_conf.baud_rate,
                    //         data_bits: serial_template_conf.data_bits,
                    //         parity: serial_template_conf.parity,
                    //     }),
                    //     metadatas: customize_conf.metadatas,
                    // }),
                    _ => unreachable!(),
                }
            }
        }

        Ok(())
    }
}

pub fn validate_conf(device_conf: &serde_json::Value) -> HaliaResult<()> {
    let device_conf: DeviceConf = serde_json::from_value(device_conf.clone())?;

    match device_conf.link_type {
        types::devices::device::modbus::LinkType::Ethernet => {
            if device_conf.ethernet.is_none() {
                return Err(HaliaError::Common("必须提供以太网的配置！".to_owned()));
            }
        }
        types::devices::device::modbus::LinkType::Serial => {
            if device_conf.serial.is_none() {
                return Err(HaliaError::Common("必须提供串口的配置！".to_owned()));
            }
        }
    }

    Ok(())
}

pub fn new_by_customize_conf(id: String, device_conf: serde_json::Value) -> Box<dyn Device> {
    let device_conf: DeviceConf = serde_json::from_value(device_conf).unwrap();
    new(id, device_conf)
}

pub fn new_by_template_conf(
    id: String,
    customize_conf: serde_json::Value,
    template_conf: serde_json::Value,
) -> Box<dyn Device> {
    let conf = Modbus::get_conf(customize_conf, template_conf).unwrap();
    new(id, conf)
}

fn new(device_id: String, device_conf: DeviceConf) -> Box<dyn Device> {
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());
    let (read_tx, read_rx) = unbounded_channel();
    let (write_tx, write_rx) = unbounded_channel();
    let (device_err_tx, _) = broadcast::channel(16);

    let sources = Arc::new(DashMap::new());
    let (err1, err2) = BiLock::new(None);

    let task_loop = TaskLoop::new(
        device_id,
        device_conf,
        err1,
        stop_signal_rx,
        sources.clone(),
        write_rx,
        read_rx,
    );

    let join_handle = task_loop.start();

    Box::new(Modbus {
        err: err2,
        sources,
        sinks: DashMap::new(),
        stop_signal_tx,
        device_err_tx,
        write_tx,
        read_tx,
        join_handle: Some(join_handle),
    })
}

pub fn validate_source_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
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
    fn get_conf(
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<DeviceConf> {
        debug!("{:?}", customize_conf);
        let customize_conf: CustomizeConf = serde_json::from_value(customize_conf)?;
        let template_conf: TemplateConf = serde_json::from_value(template_conf)?;
        match &template_conf.link_type {
            types::devices::device::modbus::LinkType::Ethernet => {
                match (customize_conf.ethernet, template_conf.ethernet) {
                    (Some(ethernet_customize_conf), Some(ethernet_template_conf)) => {
                        Ok(DeviceConf {
                            link_type: template_conf.link_type,
                            reconnect: template_conf.reconnect,
                            interval: template_conf.interval,
                            ethernet: Some(Ethernet {
                                mode: ethernet_template_conf.mode,
                                encode: ethernet_template_conf.encode,
                                host: ethernet_customize_conf.host,
                                port: ethernet_customize_conf.port,
                            }),
                            serial: None,
                            metadatas: customize_conf.metadatas,
                        })
                    }
                    _ => unreachable!(),
                }
            }
            types::devices::device::modbus::LinkType::Serial => {
                match (customize_conf.serial, template_conf.serial) {
                    (Some(serial_customize_conf), Some(serial_template_conf)) => Ok(DeviceConf {
                        link_type: template_conf.link_type,
                        reconnect: template_conf.reconnect,
                        interval: template_conf.interval,
                        ethernet: None,
                        serial: Some(Serial {
                            path: serial_customize_conf.path,
                            stop_bits: serial_template_conf.stop_bits,
                            baud_rate: serial_template_conf.baud_rate,
                            data_bits: serial_template_conf.data_bits,
                            parity: serial_template_conf.parity,
                        }),
                        metadatas: customize_conf.metadatas,
                    }),
                    _ => unreachable!(),
                }
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
            slave: customize_conf.slave,
            field: template_conf.field,
            data_type: template_conf.data_type,
            area: template_conf.area,
            address: template_conf.address,
            interval: template_conf.interval,
            metadatas: customize_conf.metadatas,
        })
    }

    fn get_sink_conf(
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<SinkConf> {
        let customize_conf: SinkCustomizeConf = serde_json::from_value(customize_conf)?;
        let template_conf: SinkTemplateConf = serde_json::from_value(template_conf)?;
        Ok(SinkConf {
            slave: customize_conf.slave,
            data_type: template_conf.data_type,
            area: template_conf.area,
            address: template_conf.address,
            value: template_conf.value,
            message_retain: template_conf.message_retain,
        })
    }

    async fn update_conf(&mut self, modbus_conf: DeviceConf) {
        self.stop_signal_tx.send(()).unwrap();
        let mut task_loop = self.join_handle.take().unwrap().await.unwrap();
        task_loop.device_conf = modbus_conf;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
    }

    fn create_source(&mut self, source_id: String, conf: SourceConf) {
        let source = Source::new(
            source_id.clone(),
            conf,
            self.read_tx.clone(),
            self.device_err_tx.subscribe(),
        );
        self.sources.insert(source_id, source);
    }

    async fn update_source(&mut self, source_id: &String, conf: SourceConf) -> HaliaResult<()> {
        match self.sources.get_mut(source_id) {
            Some(mut source) => {
                source.update(conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    fn create_sink(&mut self, sink_id: String, conf: SinkConf) {
        let sink = Sink::new(conf, self.write_tx.clone(), self.device_err_tx.subscribe());
        self.sinks.insert(sink_id, sink);
    }

    async fn update_sink(&mut self, sink_id: &String, conf: SinkConf) -> HaliaResult<()> {
        match self.sinks.get_mut(sink_id) {
            Some(mut sink) => {
                sink.update(conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
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

async fn write_value(
    ctx: &mut Box<dyn modbus_protocol::Context>,
    wpe: WritePointEvent,
) -> HaliaResult<()> {
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
            modbus_protocol::ModbusError::Transport(t) => Err(HaliaError::Io(t)),
            modbus_protocol::ModbusError::Protocol(e) => {
                warn!("modbus device_type err :{:?}", e);
                Ok(())
            }
            modbus_protocol::ModbusError::Exception(e) => {
                warn!("modbus exception err :{:?}", e);
                Ok(())
            }
        },
    }
}

#[async_trait]
impl Device for Modbus {
    async fn read_err(&self) -> Option<Arc<String>> {
        self.read_err().await
    }

    async fn read_source_err(&self, _source_id: &String) -> Option<String> {
        None
    }

    async fn read_sink_err(&self, _sink_id: &String) -> Option<String> {
        None
    }

    async fn update_customize_conf(&mut self, device_conf: serde_json::Value) -> HaliaResult<()> {
        let device_conf: DeviceConf = serde_json::from_value(device_conf)?;
        self.update_conf(device_conf).await;
        Ok(())
    }

    async fn update_template_conf(&mut self, template_conf: serde_json::Value) -> HaliaResult<()> {
        self.stop_signal_tx.send(()).unwrap();
        let mut task_loop = self.join_handle.take().unwrap().await.unwrap();
        task_loop.update_template_conf(template_conf)?;
        let join_handle = task_loop.start();
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
        self.create_source(source_id, conf);
        Ok(())
    }

    async fn create_template_source(
        &mut self,
        source_id: String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf = Self::get_source_conf(customize_conf, template_conf)?;
        self.create_source(source_id, conf);
        Ok(())
    }

    async fn update_customize_source(
        &mut self,
        source_id: &String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        self.update_source(source_id, conf).await
    }

    async fn update_template_source(
        &mut self,
        source_id: &String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf = Self::get_source_conf(customize_conf, template_conf)?;
        self.update_source(source_id, conf).await
    }

    async fn write_source_value(&mut self, source_id: String, req: Value) -> HaliaResult<()> {
        if let Some(err) = self.err.lock().await.as_ref() {
            return Err(HaliaError::Common(err.to_string()));
        }

        match self.sources.get(&source_id) {
            Some(source) => {
                match WritePointEvent::new(
                    source.source_conf.slave,
                    source.source_conf.area.clone(),
                    source.source_conf.address,
                    source.source_conf.data_type.clone(),
                    req.value,
                ) {
                    Ok(wpe) => {
                        match self.write_tx.send(wpe) {
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
        self.create_sink(sink_id, conf);
        Ok(())
    }

    async fn create_template_sink(
        &mut self,
        sink_id: String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf = Self::get_sink_conf(customize_conf, template_conf)?;
        self.create_sink(sink_id, conf);
        Ok(())
    }

    async fn update_customize_sink(
        &mut self,
        sink_id: &String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        self.update_sink(sink_id, conf).await
    }

    async fn update_template_sink(
        &mut self,
        sink_id: &String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf = Self::get_sink_conf(customize_conf, template_conf)?;
        self.update_sink(sink_id, conf).await
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

    async fn get_source_rxs(
        &self,
        source_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<UnboundedReceiver<RuleMessageBatch>>> {
        match self.sources.get_mut(source_id) {
            Some(mut source) => Ok(source.get_rxs(cnt)),
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn get_sink_txs(
        &self,
        sink_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<UnboundedSender<RuleMessageBatch>>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.get_txs(cnt)),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}
