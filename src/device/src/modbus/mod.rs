use anyhow::{bail, Result};
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use group::Group;
use group_point::Area;
use message::{MessageBatch, MessageValue};
use protocol::modbus::{
    client::{rtu, tcp, Context, Writer},
    SlaveContext,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sink::Sink;
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
    sync::{mpsc, RwLock},
    time,
};
use tokio_serial::{DataBits, Parity, SerialPort, SerialStream, StopBits};
use tracing::{debug, warn};
use types::device::{
    datatype::{DataType, Endian},
    device::{Mode, SearchDeviceItemResp, SearchSinksResp, UpdateDeviceReq},
    group::SearchGroupResp,
    point::SearchPointResp,
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
    on: Arc<AtomicBool>,  // true:开启 false:关闭
    err: Arc<AtomicBool>, // true:错误 false:正常

    stop_signal_tx: Option<mpsc::Sender<()>>,

    rtt: Arc<AtomicU16>,
    conf: Conf,
    write_tx: Option<mpsc::Sender<WritePointEvent>>,
    ref_cnt: usize,

    groups: Arc<RwLock<Vec<Group>>>,
    read_tx: Option<mpsc::Sender<Uuid>>,

    sinks: Vec<Sink>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
struct Conf {
    name: String,
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
}

impl Modbus {
    pub async fn new(device_id: Option<Uuid>, data: &String) -> HaliaResult<Modbus> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

        let conf: Conf = serde_json::from_str(data)?;
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

        if new {
            persistence::modbus::create(&device_id, &data).await?;
        }

        Ok(Modbus {
            id: device_id,
            on: Arc::new(AtomicBool::new(false)),
            err: Arc::new(AtomicBool::new(false)),
            rtt: Arc::new(AtomicU16::new(9999)),
            conf,
            groups: Arc::new(RwLock::new(vec![])),
            read_tx: None,
            write_tx: None,
            ref_cnt: 0,
            sinks: vec![],
            stop_signal_tx: None,
        })
    }

    pub async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::modbus::read_groups(&self.id).await {
            Ok(groups) => {
                for (group_id, data) in groups {
                    self.create_group(Some(group_id), data).await?;
                }
                for group in self.groups.write().await.iter_mut() {
                    group.recover(&self.id).await?;
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn search(&self) -> SearchDeviceItemResp {
        SearchDeviceItemResp {
            id: self.id,
            name: self.conf.name.clone(),
            r#type: TYPE,
            rtt: self.rtt.load(Ordering::SeqCst),
            on: self.on.load(Ordering::SeqCst),
            err: self.err.load(Ordering::SeqCst),
            conf: json!(&self.conf),
        }
    }

    pub async fn update(&mut self, data: String) -> HaliaResult<()> {
        let update_conf: Conf = serde_json::from_str(&data)?;

        let mut restart = false;
        if (self.conf.ethernet != update_conf.ethernet || self.conf.serial != update_conf.serial)
            && self.on.load(Ordering::SeqCst)
        {
            restart = true;
        }

        self.conf = update_conf;
        if restart {
            self.stop(true).await?;
            self.start(true).await?;
        }

        persistence::device::update_device_conf(&self.id, &data).await?;

        Ok(())
    }

    pub async fn start(&mut self, restart: bool) -> HaliaResult<()> {
        if let Err(_) = self
            .on
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        {
            return Ok(());
        }

        debug!("设备开启");

        if !restart {
            persistence::device::update_device_status(&self.id, Status::Runing).await?;
        }

        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);
        let (read_tx, mut read_rx) = mpsc::channel::<Uuid>(16);
        self.read_tx = Some(read_tx);
        let (write_tx, mut write_rx) = mpsc::channel::<WritePointEvent>(16);
        self.write_tx = Some(write_tx);

        let conf = self.conf.clone();
        let groups = self.groups.clone();
        let on = self.on.clone();
        let err = self.err.clone();
        let rtt = self.rtt.clone();
        tokio::spawn(async move {
            loop {
                match Modbus::get_context(&conf).await {
                    Ok((mut ctx, interval)) => {
                        err.store(false, Ordering::SeqCst);
                        loop {
                            select! {
                                biased;
                                _ = stop_signal_rx.recv() => {
                                    debug!("here");
                                    on.store(false, Ordering::SeqCst);
                                    return
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

                if !on.load(Ordering::SeqCst) {
                    debug!("device stoped");
                    return;
                }
                time::sleep(Duration::from_secs(3)).await;
            }
        });

        for group in self.groups.write().await.iter_mut() {
            let read_tx = self.read_tx.as_ref().unwrap().clone();
            group.start(read_tx, self.err.clone());
        }

        Ok(())
    }

    pub async fn stop(&mut self, restart: bool) -> HaliaResult<()> {
        if let Err(_) = self
            .on
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
        {
            return Ok(());
        }

        debug!("设备停止");
        if !restart {
            persistence::device::update_device_status(&self.id, Status::Stopped).await?;
        }

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

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        // TODO
        // if let Err(_) = self
        //     .on
        //     .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
        // {
        //     return;
        // }

        debug!("设备删除");
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

        Ok(())
    }

    async fn get_context(conf: &Conf) -> HaliaResult<(Context, u64)> {
        if let Some(conf) = &conf.ethernet {
            let socket_addr: SocketAddr = format!("{}:{}", conf.ip, conf.port).parse().unwrap();
            let transport = TcpStream::connect(socket_addr).await?;
            match conf.encode {
                Encode::Tcp => Ok((tcp::attach(transport), conf.interval)),
                Encode::Rtu => Ok((rtu::attach(transport), conf.interval)),
            }
        } else if let Some(conf) = &conf.serial {
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

    pub async fn create_group(&mut self, group_id: Option<Uuid>, data: String) -> HaliaResult<()> {
        match Group::new(&self.id, group_id, data).await {
            Ok(mut group) => {
                if self.on.load(Ordering::SeqCst) {
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

    pub async fn search_groups(&self, page: usize, size: usize) -> HaliaResult<SearchGroupResp> {
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
        Ok(SearchGroupResp {
            total: self.groups.read().await.len(),
            data: resps,
        })
    }

    pub async fn update_group(&mut self, group_id: Uuid, data: String) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => match group.update(data) {
                Ok(restart) => {
                    if restart && self.on.load(Ordering::SeqCst) {
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
        if self.on.load(Ordering::SeqCst) {
            match self
                .groups
                .write()
                .await
                .iter_mut()
                .find(|group| group.id == group_id)
            {
                Some(group) => {
                    group.stop().await;
                    self.groups
                        .write()
                        .await
                        .retain(|group| group.id != group_id);
                }
                None => return Err(HaliaError::NotFound),
            }
        }

        Ok(())
    }

    pub async fn create_group_point(
        &mut self,
        group_id: Uuid,
        point_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.create_point(&self.id, point_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_group_points(
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

    pub async fn update_group_point(
        &mut self,
        group_id: Uuid,
        point_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update_point(&self.id, point_id, data).await,
            None => {
                debug!("未找到组");
                Err(HaliaError::NotFound)
            }
        }
    }

    pub async fn write_point_value(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        value: String,
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
            Some(group) => Ok(group.delete_points(point_ids).await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_sink(&mut self, sink_id: Option<Uuid>, data: String) -> HaliaResult<()> {
        todo!()
    }

    pub async fn search_sinks(&self, page: usize, size: usize) -> HaliaResult<SearchSinksResp> {
        todo!()
    }

    pub async fn update_sink(&mut self, sink_id: Uuid, data: String) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        todo!()
    }
}

// async fn subscribe(
//     &mut self,
//     group_id: &Uuid,
// ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
//     self.ref_cnt += 1;
//     match self
//         .groups
//         .write()
//         .await
//         .iter_mut()
//         .find(|group| group.id == *group_id)
//     {
//         Some(group) => Ok(group.subscribe()),
//         None => todo!(),
//     }
// }

// async fn unsubscribe(&mut self, group_id: Uuid) -> HaliaResult<()> {
//     self.ref_cnt -= 1;
//     match self
//         .groups
//         .write()
//         .await
//         .iter_mut()
//         .find(|group| group.id == group_id)
//     {
//         Some(group) => Ok(group.unsubscribe()),
//         None => todo!(),
//     }
// }

// async fn create_sink(&mut self, sink_id: Uuid, data: &String) -> HaliaResult<()> {
//     let sink = Sink::new(sink_id, data)?;
//     self.sinks.push(sink);
//     Ok(())
// }

// async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp {
//     let mut data = vec![];
//     let mut i = 0;
//     for sink in self.sinks.iter().rev().skip((page - 1) * size) {
//         data.push(sink.get_info().await);
//         i += 1;
//         if i >= size {
//             break;
//         }
//     }
//     SearchSinksResp {
//         total: self.sinks.len(),
//         data,
//     }
// }

// async fn update_sink(&mut self, sink_id: Uuid, data: &String) -> HaliaResult<()> {
//     match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
//         Some(sink) => {
//             // let mut points = vec![];
//             // for point_conf in &conf.points {
//             //     let point = Point::new(point_conf)?;
//             //     points.push(point);
//             // }
//             // *sink.points.write().await = points;
//             // sink.conf = conf;
//             Ok(())
//         }
//         None => Err(HaliaError::NotFound),
//     }
// }

// async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
//     // TODO stop
//     self.sinks.retain(|sink| sink.id != sink_id);
//     Ok(())
// }

// async fn create_sink_item(
//     &mut self,
//     sink_id: Uuid,
//     item_id: Uuid,
//     data: &String,
// ) -> HaliaResult<()> {
//     match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
//         Some(sink) => match sink.create_point(item_id, data).await {
//             Ok(_) => {
//                 sink.stop().await;
//                 sink.start(self.write_tx.as_ref().unwrap().clone());
//                 Ok(())
//             }
//             Err(e) => Err(e),
//         },
//         None => Err(HaliaError::NotFound),
//     }
// }

// async fn publish(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
//     match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
//         Some(sink) => {
//             if sink.on.load(Ordering::SeqCst) {
//                 sink.start(self.write_tx.as_ref().unwrap().clone());
//                 sink.publish()
//             } else {
//                 Err(HaliaError::NotFound)
//             }
//         }
//         None => Err(HaliaError::NotFound),
//     }
// }

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
