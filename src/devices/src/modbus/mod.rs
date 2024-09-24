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
    error::{HaliaError, HaliaResult},
    storage,
};
use dashmap::DashMap;
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
        DeviceConf, SearchDevicesItemRunningInfo,
    },
    Value,
};

use crate::{add_device_running_count, sub_device_running_count, Device};

mod sink;
mod source;

#[derive(Debug)]
struct Modbus {
    pub id: String,

    sources: Arc<DashMap<String, Source>>,
    sinks: DashMap<String, Sink>,

    stop_signal_tx: mpsc::Sender<()>,
    device_err_tx: broadcast::Sender<bool>,
    err: Arc<RwLock<Option<String>>>,
    rtt: Arc<AtomicU16>,

    write_tx: mpsc::Sender<WritePointEvent>,
    read_tx: mpsc::Sender<String>,

    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Receiver<WritePointEvent>,
            mpsc::Receiver<String>,
        )>,
    >,
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: ModbusConf = serde_json::from_value(conf.clone())?;

    if conf.ethernet.is_none() && conf.serial.is_none() {
        return Err(HaliaError::Common(
            "必须提供以太网或串口的配置！".to_owned(),
        ));
    }

    Ok(())
}

pub fn new(device_id: String, device_conf: DeviceConf) -> Box<dyn Device> {
    let conf: ModbusConf = serde_json::from_value(device_conf.ext).unwrap();

    let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
    let (read_tx, read_rx) = mpsc::channel::<String>(16);
    let (write_tx, write_rx) = mpsc::channel::<WritePointEvent>(16);
    let (device_err_tx, _) = broadcast::channel(16);

    let mut device = Modbus {
        id: device_id,
        err: Arc::new(RwLock::new(None)),
        rtt: Arc::new(AtomicU16::new(0)),
        sources: Arc::new(DashMap::new()),
        sinks: DashMap::new(),
        stop_signal_tx,
        device_err_tx,
        write_tx,
        read_tx,
        join_handle: None,
    };

    device.event_loop(conf, stop_signal_rx, read_rx, write_rx);

    Box::new(device)
}

impl Modbus {
    fn event_loop(
        &mut self,
        modbus_conf: ModbusConf,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut read_rx: mpsc::Receiver<String>,
        mut write_rx: mpsc::Receiver<WritePointEvent>,
    ) {
        let device_id = self.id.clone();
        let err = self.err.clone();
        let sources = self.sources.clone();
        let rtt = self.rtt.clone();
        let join_handle = tokio::spawn(async move {
            loop {
                match Modbus::connect(&modbus_conf).await {
                    Ok(mut ctx) => {
                        add_device_running_count();
                        if let Err(e) = storage::event::insert(
                            types::events::ResourceType::Device,
                            &device_id,
                            types::events::EventType::Connect.into(),
                            None,
                        )
                        .await
                        {
                            warn!("create event failed: {}", e);
                        }
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
                                    if modbus_conf.interval > 0 {
                                        time::sleep(Duration::from_millis(modbus_conf.interval)).await;
                                    }
                                }

                                point_id = read_rx.recv() => {
                                   if let Some(point_id) = point_id {
                                        match sources.get_mut(&point_id) {
                                            Some(mut source) => {
                                                let now = Instant::now();
                                                if let Err(_) = source.read(&mut ctx).await {
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
                        if let Err(e) = storage::event::insert(
                            types::events::ResourceType::Device,
                            &device_id,
                            types::events::EventType::DisConnect,
                            Some(e.to_string()),
                        )
                        .await
                        {
                            warn!("create event failed: {}", e);
                        };

                        if err.read().await.is_none() {
                            sub_device_running_count();
                        }

                        *err.write().await = Some(e.to_string());
                        let sleep = time::sleep(Duration::from_secs(modbus_conf.reconnect));
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
        self.join_handle = Some(join_handle);
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
    async fn read_running_info(&self) -> SearchDevicesItemRunningInfo {
        SearchDevicesItemRunningInfo {
            err: self.err.read().await.clone(),
            rtt: self.rtt.load(Ordering::SeqCst),
        }
    }

    async fn update(&mut self, old_conf: String, new_conf: &serde_json::Value) -> HaliaResult<()> {
        let old_conf: ModbusConf = serde_json::from_str(&old_conf)?;
        let new_conf: ModbusConf = serde_json::from_value(new_conf.clone())?;
        if old_conf == new_conf {
            return Ok(());
        }

        self.stop_signal_tx.send(()).await.unwrap();
        let (stop_signal_rx, write_rx, read_rx) = self.join_handle.take().unwrap().await.unwrap();
        self.event_loop(new_conf, stop_signal_rx, read_rx, write_rx);

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        for mut source in self.sources.iter_mut() {
            source.stop().await;
        }

        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        self.stop_signal_tx.send(()).await.unwrap();

        Ok(())
    }

    async fn create_source(
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

    async fn update_source(
        &mut self,
        source_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let old_conf: SourceConf = serde_json::from_value(old_conf)?;
        let new_conf: SourceConf = serde_json::from_value(new_conf)?;
        self.sources
            .get_mut(source_id)
            .ok_or(HaliaError::NotFound(source_id.to_owned()))?
            .update(old_conf, new_conf)
            .await;

        Ok(())
    }

    async fn write_source_value(&mut self, source_id: String, req: Value) -> HaliaResult<()> {
        match self.err.read().await.as_ref() {
            Some(err) => return Err(HaliaError::Common(err.to_string())),
            None => {}
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
        self.sources
            .get_mut(source_id)
            .ok_or(HaliaError::NotFound(source_id.to_owned()))?
            .stop()
            .await;
        self.sources.remove(source_id);
        Ok(())
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
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
        self.sinks
            .get_mut(sink_id)
            .ok_or(HaliaError::NotFound(sink_id.to_owned()))?
            .update(old_conf, new_conf)
            .await;

        Ok(())
    }

    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()> {
        self.sinks
            .get_mut(sink_id)
            .ok_or(HaliaError::NotFound(sink_id.to_owned()))?
            .stop()
            .await;
        self.sinks.remove(sink_id);
        Ok(())
    }

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        Ok(self
            .sources
            .get(source_id)
            .ok_or(HaliaError::NotFound(source_id.to_owned()))?
            .mb_tx
            .subscribe())
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        Ok(self
            .sinks
            .get(sink_id)
            .ok_or(HaliaError::NotFound(sink_id.to_owned()))?
            .mb_tx
            .clone())
    }
}
