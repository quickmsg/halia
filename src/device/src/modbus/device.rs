use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use protocol::modbus::client::{rtu, tcp, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    net::TcpStream,
    select,
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tokio_serial::{DataBits, Parity, SerialPort, SerialStream, StopBits};
use tracing::{debug, error};
use types::device::{
    CreateDeviceReq, CreateGroupReq, CreatePointReq, DeviceDetailResp, ListDevicesResp,
    ListGroupsResp, ListPointResp, Mode, UpdateDeviceReq, UpdateGroupReq, WritePointValueReq,
};
use uuid::Uuid;

use crate::{storage, Device};

use super::group::{Command, Group};

static TYPE: &str = "modbus";

#[derive(Debug)]
pub(crate) struct Modbus {
    id: Uuid,
    name: String,
    on: Arc<AtomicBool>,  // true:开启 false:关闭
    err: Arc<AtomicBool>, // true:错误 false:正常
    rtt: Arc<AtomicU16>,
    conf: Conf,
    groups: Arc<RwLock<Vec<Group>>>,
    group_signal_tx: Option<broadcast::Sender<Command>>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    read_tx: Option<mpsc::Sender<Uuid>>,
    write_tx: Option<mpsc::Sender<(Uuid, Uuid, Value)>>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
#[serde(rename_all = "snake_case")]
enum Conf {
    EthernetConf(EthernetConf),
    SerialConf(SerialConf),
}

#[derive(Deserialize, Clone, Serialize, PartialEq, Debug)]
pub(crate) struct EthernetConf {
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
        match format!("{}:{}", self.ip, self.port).parse::<SocketAddr>() {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}

// TODO
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
    pub fn new(id: Uuid, req: &CreateDeviceReq) -> HaliaResult<Box<dyn Device>> {
        let conf: Conf = serde_json::from_value(req.conf.clone())?;
        Ok(Box::new(Modbus {
            id,
            name: req.name.clone(),
            on: Arc::new(AtomicBool::new(false)),
            err: Arc::new(AtomicBool::new(false)),
            rtt: Arc::new(AtomicU16::new(9999)),
            conf,
            groups: Arc::new(RwLock::new(Vec::new())),
            group_signal_tx: None,
            stop_signal_tx: None,
            read_tx: None,
            write_tx: None,
        }))
    }

    async fn run(
        &self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut read_rx: mpsc::Receiver<Uuid>,
        mut write_rx: mpsc::Receiver<(Uuid, Uuid, Value)>,
    ) {
        let conf = self.conf.clone();
        let groups = self.groups.clone();
        let err = self.err.clone();
        let on = self.on.clone();
        let group_siganl_tx = self.group_signal_tx.as_ref().unwrap().clone();
        let rtt = self.rtt.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = group_siganl_tx.send(Command::Pause) {
                    error!("send signal err:{}", e);
                }
                err.store(true, Ordering::SeqCst);
                match Modbus::get_context(&conf).await {
                    Ok((ctx, interval)) => {
                        err.store(false, Ordering::SeqCst);
                        if let Err(e) = group_siganl_tx.send(Command::Restart) {
                            error!("send signal err:{}", e);
                        }
                        run_event_loop(
                            ctx,
                            &rtt,
                            &mut stop_signal_rx,
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

    async fn get_context(conf: &Conf) -> HaliaResult<(Context, u64)> {
        match conf {
            Conf::EthernetConf(conf) => {
                let socket_addr: SocketAddr = format!("{}:{}", conf.ip, conf.port).parse().unwrap();
                let transport = TcpStream::connect(socket_addr).await?;
                match conf.encode {
                    Encode::Tcp => Ok((tcp::attach(transport), conf.interval)),
                    Encode::Rtu => Ok((rtu::attach(transport), conf.interval)),
                }
            }
            Conf::SerialConf(conf) => {
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
            }
        }
    }
}

#[async_trait]
impl Device for Modbus {
    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()> {
        let conf: Conf = serde_json::from_value(req.conf.clone())?;
        self.name = req.name.clone();
        if self.conf != conf {
            self.conf = conf;
            if self.on.load(Ordering::SeqCst) {
                self.stop().await;
                // TODO
                time::sleep(Duration::from_secs(1)).await;
                self.start().await?;
            }
        }

        Ok(())
    }

    fn get_info(&self) -> ListDevicesResp {
        ListDevicesResp {
            id: self.id,
            name: self.name.clone(),
            r#type: TYPE,
            rtt: self.rtt.load(Ordering::SeqCst),
            on: self.on.load(Ordering::SeqCst),
            err: self.err.load(Ordering::SeqCst),
        }
    }

    fn get_detail(&self) -> DeviceDetailResp {
        let link_type = match self.conf {
            Conf::EthernetConf(_) => "ethernet".to_string(),
            Conf::SerialConf(_) => "serial".to_string(),
        };
        DeviceDetailResp {
            id: self.id,
            r#type: &TYPE,
            link_type,
            name: self.name.clone(),
            conf: json!(&self.conf),
        }
    }

    async fn create_group(
        &mut self,
        group_id: Option<Uuid>,
        req: &CreateGroupReq,
    ) -> HaliaResult<()> {
        let (group_id, backup) = match group_id {
            Some(group_id) => (group_id, false),
            None => (Uuid::new_v4(), true),
        };

        if backup {
            storage::insert_group(self.id, group_id, serde_json::to_string(&req)?).await?;
        }

        let group = Group::new(self.id, group_id, &req);
        if self.on.load(Ordering::SeqCst) {
            let stop_signal = self.group_signal_tx.as_ref().unwrap().subscribe();
            let read_tx = self.read_tx.as_ref().unwrap().clone();
            group.run(stop_signal, read_tx);
        }
        self.groups.write().await.push(group);

        Ok(())
    }

    async fn delete_groups(&self, group_ids: Vec<Uuid>) -> HaliaResult<()> {
        self.groups
            .write()
            .await
            .retain(|group| !group_ids.contains(&group.id));

        storage::delete_groups(self.id, &group_ids).await?;
        for group_id in group_ids {
            match self
                .group_signal_tx
                .as_ref()
                .unwrap()
                .send(Command::Stop(group_id))
            {
                Ok(_) => {}
                Err(e) => error!("group send stop singla err:{}", e),
            }
        }

        Ok(())
    }

    async fn start(&mut self) -> HaliaResult<()> {
        debug!("here");
        if self.on.load(Ordering::SeqCst) {
            return Ok(());
        } else {
            self.on.store(true, Ordering::SeqCst);
        }

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel::<()>(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (read_tx, read_rx) = mpsc::channel::<Uuid>(20);
        self.read_tx = Some(read_tx);

        let (write_tx, write_rx) = mpsc::channel::<(Uuid, Uuid, Value)>(10);
        self.write_tx = Some(write_tx);

        let (group_signal_tx, _) = broadcast::channel::<Command>(20);
        self.group_signal_tx = Some(group_signal_tx);

        self.run(stop_signal_rx, read_rx, write_rx).await;

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
            .send(Command::StopAll);
        self.group_signal_tx = None;

        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;

        self.read_tx = None;
        self.write_tx = None;
    }

    async fn read_groups(&self) -> HaliaResult<Vec<ListGroupsResp>> {
        let mut resps = Vec::new();
        for group in self.groups.read().await.iter() {
            resps.push({
                ListGroupsResp {
                    id: group.id,
                    name: group.name.clone(),
                    interval: group.interval,
                    point_count: group.get_points_num().await as u8,
                }
            });
        }
        Ok(resps)
    }

    async fn update_group(&self, group_id: Uuid, req: &UpdateGroupReq) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => {
                group.update(&req);

                if self.on.load(Ordering::SeqCst) {
                    if let Err(e) = self
                        .group_signal_tx
                        .as_ref()
                        .unwrap()
                        .send(Command::Update(group_id, req.interval))
                    {
                        error!("group_signals send err :{}", e);
                    }
                }
            }
            None => return Err(HaliaError::NotFound),
        };

        storage::update_group(self.id, group_id, serde_json::to_string(&req)?).await?;

        Ok(())
    }

    async fn create_points(
        &self,
        group_id: Uuid,
        create_points: Vec<(Option<Uuid>, CreatePointReq)>,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.create_points(create_points).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn read_points(&self, group_id: Uuid) -> HaliaResult<Vec<ListPointResp>> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => Ok(group.read_points().await),
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
        req: &WritePointValueReq,
    ) -> HaliaResult<()> {
        let _ = self
            .write_tx
            .as_ref()
            .unwrap()
            .send((group_id, point_id, req.value.clone()))
            .await;
        Ok(())
    }

    async fn delete_points(&self, group_id: Uuid, point_ids: Vec<Uuid>) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.delete_points(point_ids).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn subscribe(&self, group_id: Uuid) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => Ok(group.subscribe()),
            None => todo!(),
        }
    }
}

async fn run_event_loop(
    mut ctx: Context,
    rtt: &Arc<AtomicU16>,
    stop_signal: &mut mpsc::Receiver<()>,
    write_rx: &mut mpsc::Receiver<(Uuid, Uuid, Value)>,
    read_rx: &mut mpsc::Receiver<Uuid>,
    groups: Arc<RwLock<Vec<Group>>>,
    interval: u64,
) {
    loop {
        select! {
            biased;
            _ = stop_signal.recv() => {
                debug!("stop event_loop");
                return
            }

            point = write_rx.recv() => {
                if let Some(point) = point {
                    // if !write_point_value(&mut ctx, &groups, point.0, point.1, point.2, rtt).await {
                        // return
                    // }
                }
                time::sleep(Duration::from_millis(interval)).await;
            }

            group_id = read_rx.recv() => {
               if let Some(group_id) = group_id {
                    if !read_group_points(&mut ctx, &groups, group_id, interval, rtt).await {
                       return
                    }
                }
            }
        }
    }
}

macro_rules! read_and_set_data {
    ($ctx:expr, $point:expr, $read_fn:ident, $address:expr, $quantity:expr) => {
        match $ctx.$read_fn($address, $quantity).await {
            Ok(res) => match res {
                Ok(data) => {
                    $point.set_data(data);
                }
                Err(e) => error!("read exception err:{}", e),
            },
            Err(e) => error!("read err:{}", e),
        }
    };
}

// TODO
async fn read_group_points(
    ctx: &mut Context,
    groups: &RwLock<Vec<Group>>,
    group_id: Uuid,
    interval: u64,
    rtt: &Arc<AtomicU16>,
) -> bool {
    if let Some(group) = groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
    {
        group.read(ctx, interval).await;
        true
    } else {
        true
    }
}

// async fn write_point_value(
//     ctx: &mut Context,
//     groups: &RwLock<Vec<Group>>,
//     group_id: Uuid,
//     point_id: Uuid,
//     value: Value,
//     rtt: &Arc<AtomicU16>,
// ) -> bool {
//     if let Some(group) = groups
//         .read()
//         .await
//         .iter()
//         .find(|group| group.id == group_id)
//     {
//         if let Some((_, point)) = group
//             .points
//             .read()
//             .await
//             .iter()
//             .find(|(id, _)| *id == point_id)
//         {
//             match point.conf.area {
//                 1 => match value.as_bool() {
//                     Some(value) => {
//                         let start_time = Instant::now();
//                         if let Err(e) = ctx.write_single_coil(point.conf.address, value).await {
//                             error!("write err:{:?}", e);
//                         };

//                         let elapsed_time = start_time.elapsed().as_millis();
//                         rtt.store(elapsed_time as u16, Ordering::SeqCst);
//                     }
//                     None => error!("value is not bool"),
//                 },
//                 4 => match point.conf.r#type {
//                     DataType::Int16(_)
//                     | DataType::Uint16(_)
//                     | DataType::Int32(_, _)
//                     | DataType::Uint32(_, _)
//                     | DataType::Int64(_, _, _, _)
//                     | DataType::Uint64(_, _, _, _)
//                     | DataType::Float32(_, _)
//                     | DataType::Float64(_, _, _, _) => match point.conf.r#type.encode(value) {
//                         Ok(data) => {
//                             {
//                                 let start_time = Instant::now();

//                                 if let Err(e) = ctx
//                                     .write_multiple_registers(point.conf.address, &data)
//                                     .await
//                                 {
//                                     error!("{}", e);
//                                 }

//                                 let elapsed_time = start_time.elapsed().as_millis();
//                                 rtt.store(elapsed_time as u16, Ordering::SeqCst);
//                             };
//                         }
//                         Err(e) => error!("{}", e),
//                     },
//                     _ => error!("数据格式错误"),
//                     // types::device::DataType::String => todo!(),
//                     // types::device::DataType::Bytes => todo!(),
//                 },
//                 _ => {
//                     error!("点位不可写")
//                 }
//             }
//         }
//         true
//     } else {
//         true
//     }
// }
