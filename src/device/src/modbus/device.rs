use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU16, AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    net::TcpStream,
    select,
    sync::{
        broadcast,
        mpsc::{self, Receiver, Sender},
        RwLock,
    },
    time,
};
use tokio_modbus::{
    client::{rtu, tcp, Context, Reader},
    slave::SlaveContext,
    Slave,
};
use tokio_serial::SerialStream;
use tracing::{debug, error, trace};
use types::device::{
    CreateDeviceReq, CreateGroupReq, CreatePointReq, DeviceDetailResp, ListDevicesResp,
    ListGroupsResp, ListPointResp, Mode,
};

use crate::{storage, Device};

use super::{group::Group, point::PointConf};

static TYPE: &str = "modbus";

pub(crate) struct Modbus {
    id: u64,
    status: Arc<AtomicU8>, // 1:停止 2:运行中 3:错误
    rtt: Arc<AtomicU16>,
    conf: Conf,
    auto_increment_id: AtomicU64,
    groups: Arc<RwLock<Vec<Group>>>,
    read_tx: Option<Sender<u64>>,
    group_signals_sender: Option<broadcast::Sender<u64>>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Link {
    Serial,
    Ethernet,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
enum Encode {
    Tcp,
    Rtu,
}

#[derive(Deserialize, Serialize, Clone)]
struct Conf {
    name: String,
    link: Link,
    interval: u64,
    conf: Value,
}

#[derive(Deserialize, Clone, Serialize)]
pub(crate) struct TcpConf {
    mode: Mode,
    encode: Encode,
    max_retry: u8,
    command_interval: u64,
    ip: String,
    port: u16,
    timeout: u64,
}

#[derive(Deserialize, Clone, Serialize)]
struct RtuConf {
    max_retry: u8,
    command_interval: u64,
    path: String,
    stop_bits: u8,
    baund_rate: u32,
    data_bits: u8,
    parity: u8,
    port: u16,
    timeout: u64,
}

impl Modbus {
    pub fn new(create_device: &CreateDeviceReq, id: u64) -> Result<Box<dyn Device>> {
        let conf: Conf = serde_json::from_value(create_device.conf.clone())?;
        Ok(Box::new(Modbus {
            id,
            status: Arc::new(AtomicU8::new(1)),
            rtt: Arc::new(AtomicU16::new(9999)),
            conf,
            groups: Arc::new(RwLock::new(Vec::new())),
            read_tx: None,
            group_signals_sender: None,
            auto_increment_id: AtomicU64::new(1),
        }))
    }

    async fn insert_group(&mut self, req: CreateGroupReq, group_id: u64) -> Result<()> {
        let group = Group::new(self.id, group_id, &req);

        let interval = group.interval;
        storage::insert_group(self.id, group_id, serde_json::to_string(&req)?).await?;
        self.groups.write().await.push(group);
        match self.status.load(Ordering::SeqCst) {
            2 => match &self.group_signals_sender {
                Some(sender) => {
                    let rx = sender.subscribe();
                    let timer_group_id = group_id;
                    let interval = interval;
                    if let Some(read_tx) = &self.read_tx {
                        run_group_timer(
                            timer_group_id,
                            interval,
                            self.status.clone(),
                            rx,
                            read_tx.clone(),
                        );
                    }
                }
                None => bail!("系统BUG"),
            },
            _ => {}
        }

        Ok(())
    }

    async fn run(&mut self) {
        let conf = self.conf.clone();
        let status = self.status.clone();
        let groups = self.groups.clone();

        let (read_tx, mut read_rx) = mpsc::channel::<u64>(100);
        self.read_tx = Some(read_tx.clone());

        let interval = self.conf.interval;
        let rtt = self.rtt.clone();

        let (write_tx, mut write_rx) = mpsc::channel::<PointConf>(100);

        tokio::spawn(async move {
            loop {
                match Modbus::get_context(&conf).await {
                    Ok(ctx) => {
                        status.store(2, Ordering::SeqCst);
                        Modbus::event_loop(
                            ctx,
                            rtt.clone(),
                            status.clone(),
                            &mut write_rx,
                            &mut read_rx,
                            groups.clone(),
                            interval,
                        )
                        .await;
                    }
                    Err(e) => error!("{}", e),
                }

                let now_status = status.load(Ordering::SeqCst);
                if now_status == 1 {
                    debug!("device stoped");
                    return;
                }
                time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    async fn get_context(conf: &Conf) -> Result<Context> {
        match conf.link {
            Link::Ethernet => {
                let conf: TcpConf = serde_json::from_value(conf.conf.clone())?;
                let socket_addr: SocketAddr = format!("{}:{}", conf.ip, conf.port).parse()?;
                match conf.encode {
                    Encode::Tcp => {
                        let ctx = tcp::connect(socket_addr).await?;
                        Ok(ctx)
                    }
                    Encode::Rtu => {
                        let transport = TcpStream::connect(socket_addr).await?;
                        Ok(rtu::attach(transport))
                    }
                }
            }
            Link::Serial => {
                let conf: RtuConf = serde_json::from_value(conf.conf.clone())?;
                let builder = tokio_serial::new(conf.path, conf.baund_rate);
                let port = SerialStream::open(&builder)?;
                Ok(rtu::attach(port))
            }
        }
    }

    async fn event_loop(
        mut ctx: Context,
        rtt: Arc<AtomicU16>,
        status: Arc<AtomicU8>,
        write_rx: &mut Receiver<PointConf>,
        read_rx: &mut Receiver<u64>,
        groups: Arc<RwLock<Vec<Group>>>,
        interval: u64,
    ) {
        loop {
            // select! {
            let now_status = status.load(Ordering::SeqCst);
            if now_status == 1 || now_status == 3 {
                debug!("event is stop");
                return;
            }
            // match write_rx.try_recv() {
            //     Ok(request) => match request {
            //         Request::ReadCoils(_, _) => todo!(),
            //         Request::ReadDiscreteInputs(_, _) => todo!(),
            //         Request::WriteSingleCoil(_, _) => todo!(),
            //         Request::ReadInputRegisters(_, _) => todo!(),
            //         Request::ReadHoldingRegisters(_, _) => todo!(),
            //         Request::WriteSingleRegister(_, _) => todo!(),
            //         Request::WriteMultipleRegisters(_, _) => todo!(),
            //         Request::Disconnect => todo!(),
            //     },
            //     Err(_) => {}
            // }

            match read_rx.recv().await {
                Some(group_id) => {
                    if let Some(group) = groups
                        .write()
                        .await
                        .iter_mut()
                        .find(|group| group.id == group_id)
                    {
                        debug!("read group:{}", group.name);
                        for point in group.points.write().await.iter_mut() {
                            ctx.set_slave(Slave(point.slave));
                            let start_time = Instant::now();
                            match point.area {
                                0 => match ctx
                                    .read_discrete_inputs(point.address, point.quantity)
                                    .await
                                {
                                    Ok(data) => {
                                        let bytes: Vec<u8> =
                                            data.iter().fold(vec![], |mut x, elem| {
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
                                            return;
                                        }
                                    },
                                },
                                1 => match ctx.read_coils(point.address, point.quantity).await {
                                    Ok(data) => {
                                        let bytes: Vec<u8> =
                                            data.iter().fold(vec![], |mut x, elem| {
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
                                            return;
                                        }
                                    },
                                },
                                4 => match ctx
                                    .read_input_registers(point.address, point.quantity)
                                    .await
                                {
                                    Ok(data) => {
                                        let bytes: Vec<u8> =
                                            data.iter().fold(vec![], |mut x, elem| {
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
                                            return;
                                        }
                                    },
                                },
                                3 => match ctx
                                    .read_holding_registers(point.address, point.quantity)
                                    .await
                                {
                                    Ok(data) => {
                                        let bytes: Vec<u8> =
                                            data.iter().fold(vec![], |mut x, elem| {
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
                                            return;
                                        }
                                    },
                                },
                                _ => unreachable!(),
                            }

                            rtt.store(start_time.elapsed().as_micros() as u16, Ordering::SeqCst);
                            time::sleep(Duration::from_millis(interval)).await;
                        }
                    }
                }
                None => {}
            }
        }
        // }
        // }
    }
}

#[async_trait]
impl Device for Modbus {
    fn get_id(&self) -> u64 {
        self.id
    }

    async fn update(&mut self, conf: Value) -> Result<()> {
        let conf: Conf = serde_json::from_value(conf)?;
        self.stop().await;
        self.conf = conf;
        self.start().await?;

        Ok(())
    }

    fn get_info(&self) -> ListDevicesResp {
        ListDevicesResp {
            id: self.id,
            name: self.conf.name.clone(),
            status: self.status.load(Ordering::SeqCst),
            rtt: self.rtt.load(Ordering::SeqCst),
            r#type: TYPE,
        }
    }

    fn get_detail(&self) -> DeviceDetailResp {
        DeviceDetailResp {
            id: self.id,
            r#type: &TYPE,
            name: self.conf.name.clone(),
            conf: json!(&self.conf),
        }
    }

    async fn create_group(&mut self, req: CreateGroupReq) -> Result<()> {
        let id = self.auto_increment_id.fetch_add(1, Ordering::SeqCst);
        self.insert_group(req, id).await
    }

    // async fn recover_group(&self, record: GroupRecord) -> Result<()> {
    //     let now_id = self.auto_increment_id.load(Ordering::SeqCst);
    //     if record.id > now_id {
    //         self.auto_increment_id.store(record.id, Ordering::SeqCst);
    //     }
    //     self.insert_group(record.req, record.id).await
    // }

    async fn delete_groups(&self, group_ids: Vec<u64>) -> Result<()> {
        self.groups
            .write()
            .await
            .retain(|group| !group_ids.contains(&group.id));

        storage::delete_groups(self.id, &group_ids).await?;
        match &self.group_signals_sender {
            Some(sender) => {
                for group_id in group_ids {
                    match sender.send(group_id) {
                        Ok(_) => {}
                        Err(e) => error!("group send stop singla err:{:?}", e),
                    }
                }
            }
            None => {}
        }

        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        self.status.store(2, Ordering::SeqCst);
        self.run().await;
        Ok(())
    }

    async fn stop(&mut self) {
        self.status.store(1, Ordering::SeqCst);
        match &self.group_signals_sender {
            Some(sender) => sender.send(0).unwrap(),
            None => unreachable!(),
        };
        self.group_signals_sender = None;
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

    // TODO
    async fn update_group(&self, group_id: u64, update_group: Value) -> Result<()> {
        let update_data = serde_json::to_string(&update_group)?;

        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => match group.update(update_group) {
                Ok(interval_changed) => {
                    // if interval_changed {
                    //     match self.group_signals.lock().await.remove(&group_id) {
                    //         Some(stop_signal) => {
                    //             stop_signal.store(true, Ordering::SeqCst);
                    //         }
                    //         None => {}
                    //     }

                    //     let stop_signal = Arc::new(AtomicBool::new(false));
                    //     let mut signals = self.group_signals.lock().await;
                    //     signals.insert(group.id, stop_signal.clone());

                    //     if let Some(read_tx) = &self.read_tx {
                    //         run_group_timer(
                    //             group.id.clone(),
                    //             group.interval,
                    //             stop_signal.clone(),
                    //             read_tx.clone(),
                    //         );
                    //     };
                    // }
                }
                Err(e) => bail!("{}", e),
            },
            None => bail!("未找到组：{}。", group_id),
        }

        storage::update_group(self.id, group_id, update_data).await?;

        Ok(())
    }

    async fn create_points(&self, group_id: u64, create_points: Vec<CreatePointReq>) -> Result<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.create_points(create_points).await,
            None => bail!("没有找到组"),
        }
    }

    async fn read_points(&self, group_id: u64) -> Result<Vec<ListPointResp>> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => Ok(group.read_points().await),
            None => bail!("未找到组。"),
        }
    }

    async fn update_point(&self, group_id: u64, point_id: u64, update_point: Value) -> Result<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update_point(point_id, update_point).await,
            None => bail!("未找到组:{}。", group_id),
        }
    }

    async fn delete_points(&self, group_id: u64, point_ids: Vec<u64>) -> Result<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.delete_points(point_ids).await,
            None => bail!("没有找到组"),
        }
    }
}

fn run_group_timer(
    group_id: u64,
    interval: u64,
    device_status: Arc<AtomicU8>,
    mut stop_signal: broadcast::Receiver<u64>,
    tx: mpsc::Sender<u64>,
) {
    trace!("group {} is runing", group_id);
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(interval));
        loop {
            select! {
                _ = interval.tick() => {
                    if device_status.load(Ordering::SeqCst) == 2 {
                        match tx.send(group_id).await {
                            Ok(_) => {}
                            Err(e) => debug!("group send point info err :{}", e),
                        }
                    }
                },
                signal = stop_signal.recv() => {
                    match signal {
                        Ok(id) => {
                            if id == group_id || id == 0 {
                                debug!("group {} stop.", group_id);
                                return;
                            }
                        }
                        Err(e) => error!("group recv stop signal err :{:?}", e),
                    }
                }
            }
        }
    });
}

async fn run_event_loop(
    mut ctx: Context,
    rtt: Arc<AtomicU16>,
    mut stop_signal: mpsc::Receiver<()>,
    write_rx: &mut Receiver<PointConf>,
    read_rx: &mut Receiver<u64>,
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
                    debug!("{:?}", point);
                }

           group_id = read_rx.recv() => {
            if let Some(group_id) = group_id {
                    if let Some(group) = groups
                        .write()
                        .await
                        .iter_mut()
                        .find(|group| group.id == group_id)
                    {
                        debug!("read group:{}", group.name);
                        for point in group.points.write().await.iter_mut() {
                            ctx.set_slave(Slave(point.slave));
                            let start_time = Instant::now();
                            match point.area {
                                0 => match ctx
                                    .read_discrete_inputs(point.address, point.quantity)
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
                                            return;
                                        }
                                    },
                                },
                                1 => match ctx.read_coils(point.address, point.quantity).await {
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
                                            return;
                                        }
                                    },
                                },
                                4 => match ctx
                                    .read_input_registers(point.address, point.quantity)
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
                                            return;
                                        }
                                    },
                                },
                                3 => match ctx
                                    .read_holding_registers(point.address, point.quantity)
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
                                            return;
                                        }
                                    },
                                },
                                _ => unreachable!(),
                            }

                            rtt.store(start_time.elapsed().as_micros() as u16, Ordering::SeqCst);
                            time::sleep(Duration::from_millis(interval)).await;
                        }
                    }
            }
        }
        }
    }
    // }
}
