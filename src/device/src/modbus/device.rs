use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use common::error::{HaliaError, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    net::TcpStream,
    select,
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tokio_modbus::{
    client::{rtu, tcp, Context, Reader},
    slave::SlaveContext,
    Slave,
};
use tokio_serial::{DataBits, Parity, SerialPort, SerialStream, StopBits};
use tracing::{debug, error, trace};
use types::device::{
    CreateDeviceReq, CreateGroupReq, CreatePointReq, DeviceDetailResp, ListDevicesResp,
    ListGroupsResp, ListPointResp, Mode, UpdateDeviceReq, UpdateGroupReq,
};
use uuid::Uuid;

use crate::{storage, Device};

use super::{group::Group, point};

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

    group_signal_tx: Option<broadcast::Sender<Option<Uuid>>>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    read_tx: Option<mpsc::Sender<Uuid>>,
    write_tx: Option<mpsc::Sender<point::Conf>>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
enum Encode {
    Tcp,
    Rtu,
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
    baund_rate: u32,
    data_bits: u8,
    parity: u8,
}

impl Modbus {
    pub fn new(id: Uuid, req: &CreateDeviceReq) -> Result<Box<dyn Device>> {
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
        mut write_rx: mpsc::Receiver<point::Conf>,
    ) {
        let conf = self.conf.clone();
        let groups = self.groups.clone();

        let err = self.err.clone();
        let on = self.on.clone();

        tokio::spawn(async move {
            loop {
                err.store(true, Ordering::SeqCst);
                match Modbus::get_context(&conf).await {
                    Ok((ctx, interval)) => {
                        err.store(false, Ordering::SeqCst);
                        run_event_loop(
                            ctx,
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

    async fn get_context(conf: &Conf) -> Result<(Context, u64)> {
        match conf {
            Conf::EthernetConf(conf) => {
                let socket_addr: SocketAddr = format!("{}:{}", conf.ip, conf.port).parse().unwrap();
                match conf.encode {
                    Encode::Tcp => {
                        let ctx = tcp::connect(socket_addr).await?;
                        Ok((ctx, conf.interval))
                    }
                    Encode::Rtu => {
                        let transport = TcpStream::connect(socket_addr).await?;
                        Ok((rtu::attach(transport), conf.interval))
                    }
                }
            }
            Conf::SerialConf(conf) => {
                let builder = tokio_serial::new(conf.path.clone(), conf.baund_rate);
                let mut port = SerialStream::open(&builder).unwrap();
                port.set_baud_rate(conf.baund_rate).unwrap();
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
                    1 => port.set_parity(Parity::None).unwrap(),
                    2 => port.set_parity(Parity::Odd).unwrap(),
                    3 => port.set_parity(Parity::Even).unwrap(),
                    _ => unreachable!(),
                };

                Ok((rtu::attach(port), conf.interval))
            }
        }
    }

    fn run_group_timer(&self, group_id: Uuid, interval: u64) {
        let mut stop_signal = self.group_signal_tx.as_ref().unwrap().subscribe();
        let read_tx = self.read_tx.as_ref().unwrap().clone();
        let err = self.err.clone();

        trace!("group {} is runing", group_id);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    signal = stop_signal.recv() => {
                        match signal {
                            Ok(id) => {
                                match id {
                                    Some(id) => if id == group_id {
                                        debug!("group {} stop.", group_id);
                                        return;
                                    }
                                    None => {
                                        debug!("group {} stop.", group_id);
                                        return;
                                    }
                                }
                            }
                            Err(e) => error!("group recv stop signal err :{:?}", e),
                        }
                    }

                    _ = interval.tick() => {
                        if !err.load(Ordering::SeqCst) {
                            match read_tx.send(group_id).await {
                                Ok(_) => {}
                                Err(e) => debug!("group send point info err :{}", e),
                            }
                        }
                    }
                }
            }
        });
    }
}

#[async_trait]
impl Device for Modbus {
    async fn update(&mut self, req: &UpdateDeviceReq) -> Result<()> {
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
        DeviceDetailResp {
            id: self.id,
            r#type: &TYPE,
            name: self.name.clone(),
            conf: json!(&self.conf),
        }
    }

    async fn create_group(&mut self, group_id: Option<Uuid>, req: &CreateGroupReq) -> Result<()> {
        let (group_id, backup) = match group_id {
            Some(group_id) => (group_id, false),
            None => (Uuid::new_v4(), true),
        };

        if backup {
            storage::insert_group(self.id, group_id, serde_json::to_string(&req)?).await?;
        }

        let group = Group::new(self.id, group_id, &req);
        let interval = group.interval;
        self.groups.write().await.push(group);
        if self.on.load(Ordering::SeqCst) {
            self.run_group_timer(group_id, interval);
        }

        Ok(())
    }

    async fn delete_groups(&self, group_ids: Vec<Uuid>) -> Result<()> {
        self.groups
            .write()
            .await
            .retain(|group| !group_ids.contains(&group.id));

        storage::delete_groups(self.id, &group_ids).await?;
        for group_id in group_ids {
            match self.group_signal_tx.as_ref().unwrap().send(Some(group_id)) {
                Ok(_) => {}
                Err(e) => error!("group send stop singla err:{:?}", e),
            }
        }

        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        if self.on.load(Ordering::SeqCst) {
            return Ok(());
        } else {
            self.on.store(true, Ordering::SeqCst);
        }

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel::<()>(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (read_tx, read_rx) = mpsc::channel::<Uuid>(20);
        self.read_tx = Some(read_tx);

        let (write_tx, write_rx) = mpsc::channel::<point::Conf>(10);
        self.write_tx = Some(write_tx);

        self.run(stop_signal_rx, read_rx, write_rx).await;

        let (group_signal_tx, _) = broadcast::channel::<Option<Uuid>>(20);
        self.group_signal_tx = Some(group_signal_tx);
        for group in self.groups.read().await.iter() {
            self.run_group_timer(group.id, group.interval);
        }
        Ok(())
    }

    async fn stop(&mut self) {
        if !self.on.load(Ordering::SeqCst) {
            return;
        } else {
            self.on.store(false, Ordering::SeqCst);
        }

        self.group_signal_tx.as_ref().unwrap().send(None).unwrap();
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

    async fn read_groups(&self) -> Result<Vec<ListGroupsResp>> {
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

    async fn update_group(&self, group_id: Uuid, req: &UpdateGroupReq) -> Result<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => {
                let _ = group.update(&req);
                if self.on.load(Ordering::SeqCst) {
                    match self.group_signal_tx.as_ref().unwrap().send(Some(group_id)) {
                        Ok(_) => {}
                        Err(e) => error!("group_signals send err :{:?}", e),
                    }

                    self.run_group_timer(group_id, req.interval);
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
    ) -> Result<()> {
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

    async fn read_points(&self, group_id: Uuid) -> Result<Vec<ListPointResp>> {
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
    ) -> Result<()> {
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

    async fn delete_points(&self, group_id: Uuid, point_ids: Vec<Uuid>) -> Result<()> {
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
}

async fn run_event_loop(
    mut ctx: Context,
    stop_signal: &mut mpsc::Receiver<()>,
    write_rx: &mut mpsc::Receiver<point::Conf>,
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
                // TODO
                debug!("{:?}", point);
            }

            group_id = read_rx.recv() => {
               if let Some(group_id) = group_id {
                    if !read_group_points(&mut ctx, &groups, group_id, interval).await {
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
) -> bool {
    if let Some(group) = groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
    {
        for point in group.points.write().await.iter_mut() {
            ctx.set_slave(Slave(point.conf.slave));
            let start_time = Instant::now();
            match point.conf.area {
                0 => match ctx
                    .read_discrete_inputs(point.conf.address, point.quantity)
                    .await
                {
                    Ok(data) => {
                        let bytes: Vec<u8> = data.iter().fold(vec![], |mut x, elem| {
                            if *elem {
                                x.push(1);
                            } else {
                                x.push(0);
                            }
                            x
                        });
                        point.set_data(bytes);
                    }
                    Err(e) => match e.kind() {
                        std::io::ErrorKind::InvalidData => {
                            error!("返回错误:{}", e);
                        }
                        _ => {
                            error!("连接断开。");
                            return false;
                        }
                    },
                },
                1 => match ctx.read_coils(point.conf.address, point.quantity).await {
                    Ok(data) => {
                        let bytes: Vec<u8> = data.iter().fold(vec![], |mut x, elem| {
                            if *elem {
                                x.push(1);
                            } else {
                                x.push(0);
                            }
                            x
                        });
                        point.set_data(bytes);
                    }
                    Err(e) => match e.kind() {
                        std::io::ErrorKind::InvalidData => {
                            error!("返回错误:{}", e);
                        }
                        _ => {
                            error!("连接断开。");
                            return false;
                        }
                    },
                },
                4 => match ctx
                    .read_input_registers(point.conf.address, point.quantity)
                    .await
                {
                    Ok(data) => {
                        let bytes: Vec<u8> = data.iter().fold(vec![], |mut x, elem| {
                            x.push((elem & 0xff) as u8);
                            x.push((elem >> 8) as u8);
                            x
                        });
                        point.set_data(bytes);
                    }
                    Err(e) => match e.kind() {
                        std::io::ErrorKind::InvalidData => {
                            error!("返回错误:{}", e);
                        }
                        _ => {
                            error!("连接断开。");
                            return false;
                        }
                    },
                },
                3 => match ctx
                    .read_holding_registers(point.conf.address, point.quantity)
                    .await
                {
                    Ok(data) => {
                        let bytes: Vec<u8> = data.iter().fold(vec![], |mut x, elem| {
                            x.push((elem & 0xff) as u8);
                            x.push((elem >> 8) as u8);
                            x
                        });
                        point.set_data(bytes);
                    }
                    Err(e) => match e.kind() {
                        std::io::ErrorKind::InvalidData => {
                            error!("返回错误:{}", e);
                        }
                        _ => {
                            error!("连接断开。");
                            return false;
                        }
                    },
                },
                _ => unreachable!(),
            }

            let elapsed_time = start_time.elapsed().as_millis();

            time::sleep(Duration::from_millis(interval)).await;
        }
        true
    } else {
        true
    }
}
