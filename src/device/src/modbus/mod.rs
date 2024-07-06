use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use common::error::{HaliaError, HaliaResult};
use group::Group;
use message::MessageBatch;
use point::Area;
use protocol::modbus::{
    client::{rtu, tcp, Context, Writer},
    SlaveContext,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sink::{Sink, SinkConf};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    select,
    sync::{broadcast, mpsc, Mutex, RwLock},
    time,
};
use tokio_serial::{DataBits, Parity, SerialPort, SerialStream, StopBits};
use tracing::{debug, error, warn};
use types::device::{
    datatype::{DataType, Endian},
    device::{CreateDeviceReq, Mode, SearchDeviceItemResp, SearchSinksResp, UpdateDeviceReq},
    group::{CreateGroupReq, SearchGroupItemResp, SearchGroupResp, UpdateGroupReq},
    point::{CreatePointReq, SearchPointResp},
};
use uuid::Uuid;

use crate::Device;

pub(crate) const TYPE: &str = "modbus";
mod group;
mod point;
mod sink;

#[derive(Debug)]
struct Modbus {
    id: Uuid,
    name: String,
    on: Arc<AtomicBool>,  // true:开启 false:关闭
    err: Arc<AtomicBool>, // true:错误 false:正常
    rtt: Arc<AtomicU16>,
    conf: Arc<Mutex<Conf>>,
    groups: Arc<RwLock<Vec<Group>>>,
    group_signal_tx: Option<broadcast::Sender<group::Command>>,
    signal_tx: Option<mpsc::Sender<Command>>,
    read_tx: Option<mpsc::Sender<Uuid>>,
    write_tx: Option<mpsc::Sender<WritePointEvent>>,
    ref_cnt: usize,

    sinks: RwLock<Vec<Sink>>,
}

#[derive(Debug)]
enum Command {
    Stop,
    Update,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
struct Conf {
    #[serde(skip_serializing_if = "Option::is_none")]
    ethernet: Option<EthernetConf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    serial: Option<SerialConf>,
}

#[derive(Deserialize, Clone, Serialize, PartialEq, Debug)]
struct EthernetConf {
    mode: Mode,
    encode: Encode,
    ip: String,
    port: u16,
    interval: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    desc: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
enum Encode {
    Tcp,
    Rtu,
}

impl EthernetConf {
    fn validate(&self) -> bool {
        if let Err(_) = self.ip.parse::<IpAddr>() {
            return false;
        }

        true
    }
}

impl SerialConf {
    fn validate(&self) -> bool {
        true
    }
}

#[derive(Deserialize, Clone, Serialize, PartialEq, Debug)]
struct SerialConf {
    interval: u64,
    path: String,
    stop_bits: u8,
    baud_rate: u32,
    data_bits: u8,
    parity: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    desc: Option<String>,
}

pub(crate) fn new(id: Uuid, req: &CreateDeviceReq) -> HaliaResult<Box<dyn Device>> {
    let conf: Conf = serde_json::from_value(req.conf.clone())?;
    if let Some(ethernet) = &conf.ethernet {
        if !ethernet.validate() {
            return Err(HaliaError::ConfErr);
        }
    } else if let Some(serial) = &conf.serial {
        if !serial.validate() {
            return Err(HaliaError::ConfErr);
        }
    } else {
        return Err(HaliaError::ConfErr);
    }

    Ok(Box::new(Modbus {
        id,
        name: req.name.clone(),
        on: Arc::new(AtomicBool::new(false)),
        err: Arc::new(AtomicBool::new(false)),
        rtt: Arc::new(AtomicU16::new(9999)),
        conf: Arc::new(Mutex::new(conf)),
        groups: Arc::new(RwLock::new(Vec::new())),
        signal_tx: None,
        group_signal_tx: None,
        read_tx: None,
        write_tx: None,
        ref_cnt: 0,

        sinks: RwLock::new(vec![]),
    }))
}

impl Modbus {
    async fn run(
        &self,
        mut signal_rx: mpsc::Receiver<Command>,
        mut read_rx: mpsc::Receiver<Uuid>,
        mut write_rx: mpsc::Receiver<WritePointEvent>,
    ) {
        let conf = self.conf.clone();
        let groups = self.groups.clone();
        let err = self.err.clone();
        let on = self.on.clone();
        let group_siganl_tx = self.group_signal_tx.as_ref().unwrap().clone();
        let rtt = self.rtt.clone();
        tokio::spawn(async move {
            loop {
                if groups.read().await.len() != 0 {
                    if let Err(e) = group_siganl_tx.send(group::Command::Pause) {
                        error!("send signal err:{}", e);
                    }
                }

                err.store(true, Ordering::SeqCst);
                match Modbus::get_context(&conf).await {
                    Ok((ctx, interval)) => {
                        err.store(false, Ordering::SeqCst);
                        if let Err(e) = group_siganl_tx.send(group::Command::Restart) {
                            error!("send signal err:{}", e);
                        }
                        run_event_loop(
                            ctx,
                            &rtt,
                            &mut signal_rx,
                            &mut write_rx,
                            &mut read_rx,
                            groups.clone(),
                            interval,
                        )
                        .await;
                    }
                    Err(_) => error!("连接失败"),
                }

                if !on.load(Ordering::SeqCst) {
                    debug!("device stoped");
                    return;
                }
                time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    async fn get_context(conf: &Arc<Mutex<Conf>>) -> HaliaResult<(Context, u64)> {
        if let Some(conf) = &conf.lock().await.ethernet {
            let socket_addr: SocketAddr = format!("{}:{}", conf.ip, conf.port).parse().unwrap();
            let transport = TcpStream::connect(socket_addr).await?;
            match conf.encode {
                Encode::Tcp => Ok((tcp::attach(transport), conf.interval)),
                Encode::Rtu => Ok((rtu::attach(transport), conf.interval)),
            }
        } else if let Some(conf) = &conf.lock().await.serial {
            let builder = tokio_serial::new(conf.path.clone(), conf.baud_rate);
            let mut port = SerialStream::open(&builder).unwrap();
            match conf.stop_bits {
                1 => port.set_stop_bits(StopBits::One).unwrap(),
                2 => port.set_stop_bits(StopBits::Two).unwrap(),
                _ => unreachable!(),
            };
            match conf.data_bits {
                5 => port.set_data_bits(DataBits::Five).unwrap(),
                6 => port.set_data_bits(DataBits::Six).unwrap(),
                7 => port.set_data_bits(DataBits::Seven).unwrap(),
                8 => port.set_data_bits(DataBits::Eight).unwrap(),
                _ => unreachable!(),
            };

            match conf.parity {
                0 => port.set_parity(Parity::None).unwrap(),
                1 => port.set_parity(Parity::Odd).unwrap(),
                2 => port.set_parity(Parity::Even).unwrap(),
                _ => unreachable!(),
            };

            Ok((rtu::attach(port), conf.interval))
        } else {
            panic!("no conf for modubs");
        }
    }

    async fn get_write_point_event(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        value: serde_json::Value,
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
}

#[async_trait]
impl Device for Modbus {
    fn get_id(&self) -> Uuid {
        self.id
    }

    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()> {
        let update_conf: Conf = serde_json::from_value(req.conf.clone())?;

        if self.name != req.name {
            self.name = req.name.clone();
        }

        *self.conf.lock().await = update_conf;
        if self.on.load(Ordering::SeqCst) {
            if let Err(e) = self.signal_tx.as_ref().unwrap().send(Command::Update).await {
                debug!("device signal command send err:{:?}", e);
            }
        }

        Ok(())
    }

    async fn get_info(&self) -> SearchDeviceItemResp {
        SearchDeviceItemResp {
            id: self.id,
            name: self.name.clone(),
            r#type: TYPE,
            rtt: self.rtt.load(Ordering::SeqCst),
            on: self.on.load(Ordering::SeqCst),
            err: self.err.load(Ordering::SeqCst),
            conf: json!(&self.conf.lock().await.clone()),
        }
    }

    async fn create_group(&mut self, group_id: Uuid, req: &CreateGroupReq) -> HaliaResult<()> {
        match Group::new(group_id, &req) {
            Ok(group) => {
                if self.on.load(Ordering::SeqCst) {
                    let stop_signal = self.group_signal_tx.as_ref().unwrap().subscribe();
                    let read_tx = self.read_tx.as_ref().unwrap().clone();
                    group.run(stop_signal, read_tx);
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

    async fn delete_group(&self, group_id: Uuid) -> HaliaResult<()> {
        self.groups
            .write()
            .await
            .retain(|group| group_id != group.id);

        if self.on.load(Ordering::SeqCst) {
            match self
                .group_signal_tx
                .as_ref()
                .unwrap()
                .send(group::Command::Stop(group_id))
            {
                Ok(_) => {}
                Err(e) => error!("group send stop singla err:{}", e),
            }
        }

        Ok(())
    }

    async fn start(&mut self) -> HaliaResult<()> {
        if self.on.load(Ordering::SeqCst) {
            return Ok(());
        } else {
            self.on.store(true, Ordering::SeqCst);
        }

        let (signal_tx, signal_rx) = mpsc::channel::<Command>(1);
        self.signal_tx = Some(signal_tx);

        let (read_tx, read_rx) = mpsc::channel::<Uuid>(20);
        self.read_tx = Some(read_tx);

        let (write_tx, write_rx) = mpsc::channel::<WritePointEvent>(16);
        self.write_tx = Some(write_tx);

        let (group_signal_tx, _) = broadcast::channel::<group::Command>(20);
        self.group_signal_tx = Some(group_signal_tx);

        self.run(signal_rx, read_rx, write_rx).await;

        for group in self.groups.read().await.iter() {
            let stop_signal = self.group_signal_tx.as_ref().unwrap().subscribe();
            let read_tx = self.read_tx.as_ref().unwrap().clone();
            group.run(stop_signal, read_tx);
        }
        Ok(())
    }

    async fn stop(&mut self) {
        if !self.on.load(Ordering::SeqCst) {
            return;
        } else {
            self.on.store(false, Ordering::SeqCst);
        }

        let _ = self
            .group_signal_tx
            .as_ref()
            .unwrap()
            .send(group::Command::StopAll);
        self.group_signal_tx = None;

        self.signal_tx
            .as_ref()
            .unwrap()
            .send(Command::Stop)
            .await
            .unwrap();
        self.signal_tx = None;

        self.read_tx = None;
        self.write_tx = None;
    }

    async fn search_groups(&self, page: usize, size: usize) -> HaliaResult<SearchGroupResp> {
        let mut resps = Vec::new();
        for group in self
            .groups
            .read()
            .await
            .iter()
            .rev()
            .skip(((page - 1) * size) as usize)
        {
            resps.push({
                SearchGroupItemResp {
                    id: group.id,
                    name: group.name.clone(),
                    interval: group.interval,
                    point_count: group.get_points_num().await as u8,
                    desc: group.desc.clone(),
                }
            });
            if resps.len() == size as usize {
                break;
            }
        }
        Ok(SearchGroupResp {
            total: self.groups.read().await.len(),
            data: resps,
        })
    }

    async fn update_group(&self, group_id: Uuid, req: UpdateGroupReq) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => {
                if group.interval != req.interval && self.on.load(Ordering::SeqCst) {
                    if let Err(e) = self
                        .group_signal_tx
                        .as_ref()
                        .unwrap()
                        .send(group::Command::Update(group_id, req.interval))
                    {
                        error!("group_signals send err :{}", e);
                    }
                }

                group.update(&req);
            }
            None => return Err(HaliaError::NotFound),
        };

        Ok(())
    }

    async fn create_point(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        req: CreatePointReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.create_point(point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn search_point(
        &self,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchPointResp> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => Ok(group.search_points(page, size).await),
            None => Err(HaliaError::NotFound),
        }
    }

    async fn update_point(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        req: &CreatePointReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update_point(point_id, req).await,
            None => {
                debug!("未找到组");
                Err(HaliaError::NotFound)
            }
        }
    }

    async fn write_point_value(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        value: serde_json::Value,
    ) -> HaliaResult<()> {
        if self.on.load(Ordering::SeqCst) == false {
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
            Err(e) => return Err(HaliaError::IoErr),
        }
    }

    async fn delete_points(&self, group_id: &Uuid, point_ids: &Vec<Uuid>) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => Ok(group.delete_points(point_ids).await),
            None => Err(HaliaError::NotFound),
        }
    }

    async fn subscribe(
        &mut self,
        group_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        self.ref_cnt += 1;
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => Ok(group.subscribe()),
            None => todo!(),
        }
    }

    async fn unsubscribe(&mut self, group_id: Uuid) -> HaliaResult<()> {
        self.ref_cnt -= 1;
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => Ok(group.unsubscribe()),
            None => todo!(),
        }
    }

    async fn create_sink(&self, sink_id: Uuid, req: &Bytes) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_slice(req)?;
        let sink = sink::new(sink_id, conf)?;
        self.sinks.write().await.push(sink);
        Ok(())
    }

    async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp {
        let mut data = vec![];
        let mut i = 0;
        for sink in self.sinks.read().await.iter().skip((page - 1) * size) {
            data.push(sink.search());
            i += 1;
            if i >= size {
                break;
            }
        }
        SearchSinksResp {
            total: self.sinks.read().await.len(),
            data,
        }
    }

    async fn update_sink(&self, sink_id: Uuid, req: &Bytes) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_sink(&self, sink_id: Uuid) -> HaliaResult<()> {
        // TODO 检查是否被引用
        self.sinks.write().await.retain(|sink| sink.id != sink_id);
        Ok(())
    }

    async fn publish(&self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self
            .sinks
            .write()
            .await
            .iter_mut()
            .find(|sink| sink.id == *sink_id)
        {
            Some(sink) => match &sink.tx {
                Some(tx) => return Ok(tx.clone()),
                None => {
                    let (tx, rx) = mpsc::channel::<MessageBatch>(16);
                    let tx_clone = tx.clone();
                    sink.tx = Some(tx);
                    sink.run(rx, self.write_tx.as_ref().unwrap().clone());
                    Ok(tx_clone)
                }
            },
            None => Err(HaliaError::NotFound),
        }
    }
}

async fn run_event_loop(
    mut ctx: Context,
    rtt: &Arc<AtomicU16>,
    signal_rx: &mut mpsc::Receiver<Command>,
    write_rx: &mut mpsc::Receiver<WritePointEvent>,
    read_rx: &mut mpsc::Receiver<Uuid>,
    groups: Arc<RwLock<Vec<Group>>>,
    interval: u64,
) {
    loop {
        select! {
            biased;
            command = signal_rx.recv() => {
                if let Some(command) = command {
                    match command {
                        Command::Stop => return,
                        Command::Update => return,
                    }
                }
            }

            wpe = write_rx.recv() => {
                if let Some(wpe) = wpe {
                    match write_value(&mut ctx, wpe).await {
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
                if interval > 0 {
                    time::sleep(Duration::from_millis(interval)).await;
                }
            }

            group_id = read_rx.recv() => {
               if let Some(group_id) = group_id {
                    if let Err(_) = read_group_points(&mut ctx, &groups, group_id, interval, rtt).await {
                       return
                    }
                }
            }
        }
    }
}

async fn read_group_points(
    ctx: &mut Context,
    groups: &RwLock<Vec<Group>>,
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
