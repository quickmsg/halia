use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{bail, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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
use tokio_serial::SerialStream;
use tracing::{debug, error, trace};
use types::device::{
    CreateDeviceReq, CreateGroupReq, CreatePointReq, DeviceDetailResp, ListDevicesResp,
    ListGroupsResp, ListPointResp, Mode,
};

use crate::{storage, Device};

use super::{
    group::{self, Group},
    point::PointConf,
};

static TYPE: &str = "modbus";

pub(crate) struct Modbus {
    on: Arc<AtomicBool>, // true:开启 false:关闭
    err: Arc<AtomicBool>,
    id: u64,
    rtt: Arc<AtomicU16>,
    conf: Conf,
    auto_increment_id: AtomicU64,
    groups: Arc<RwLock<Vec<Group>>>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    read_tx: Option<mpsc::Sender<u64>>,
    write_tx: Option<mpsc::Sender<PointConf>>,
    group_signals_sender: broadcast::Sender<u64>,
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
        let (group_signals_sender, _) = broadcast::channel::<u64>(16);
        Ok(Box::new(Modbus {
            id,
            auto_increment_id: AtomicU64::new(1),
            on: Arc::new(AtomicBool::new(false)),
            err: Arc::new(AtomicBool::new(false)),
            stop_signal_tx: None,
            rtt: Arc::new(AtomicU16::new(9999)),
            conf,
            groups: Arc::new(RwLock::new(Vec::new())),
            read_tx: None,
            group_signals_sender,
            write_tx: None,
        }))
    }

    async fn run(&mut self) -> (mpsc::Sender<()>, mpsc::Sender<u64>, mpsc::Sender<PointConf>) {
        let conf = self.conf.clone();
        let groups = self.groups.clone();

        let interval = self.conf.interval;
        let rtt = self.rtt.clone();

        let err = self.err.clone();
        let on = self.on.clone();

        let (write_tx, mut write_rx) = mpsc::channel::<PointConf>(100);

        let (stop_signal_tx, mut stop_sgianl_rx) = mpsc::channel::<()>(1);

        let (read_tx, mut read_rx) = mpsc::channel::<u64>(100);

        tokio::spawn(async move {
            loop {
                debug!("device runing");
                match Modbus::get_context(&conf).await {
                    Ok(ctx) => {
                        err.store(false, Ordering::SeqCst);
                        run_event_loop(
                            ctx,
                            rtt.clone(),
                            &mut stop_sgianl_rx,
                            &mut write_rx,
                            &mut read_rx,
                            groups.clone(),
                            interval,
                        )
                        .await;
                    }
                    Err(e) => error!("{}", e),
                }

                if !on.load(Ordering::SeqCst) {
                    debug!("device stoped");
                    return;
                }
                time::sleep(Duration::from_secs(3)).await;
            }
        });

        (stop_signal_tx, read_tx, write_tx)
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

    fn run_group_timer(&self, group_id: u64, interval: u64) {
        let mut stop_signal = self.group_signals_sender.subscribe();
        let read_tx = self.read_tx.as_ref().unwrap().clone();
        let err = self.err.clone();

        trace!("group {} is runing", group_id);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    _ = interval.tick() => {
                        if !err.load(Ordering::SeqCst) {
                            match read_tx.send(group_id).await {
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
}

#[async_trait]
impl Device for Modbus {
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
            // todo
            status: 3,
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

    async fn create_group(&mut self, group_id: Option<u64>, req: &CreateGroupReq) -> Result<()> {
        let (group_id, new) = match group_id {
            Some(group_id) => {
                if group_id > self.auto_increment_id.load(Ordering::SeqCst) {
                    self.auto_increment_id.store(group_id, Ordering::SeqCst);
                }
                (group_id, false)
            }
            None => (self.auto_increment_id.fetch_add(1, Ordering::SeqCst), true),
        };

        if new {
            debug!("here");
            storage::insert_group(self.id, group_id, serde_json::to_string(&req)?).await?;
        }

        let group = Group::new(self.id, group_id, &req);
        let interval = group.interval;
        self.groups.write().await.push(group);
        if self.on.load(Ordering::SeqCst) {
            self.run_group_timer(group_id, interval);
        }

        debug!("create group done");

        Ok(())
    }

    async fn delete_groups(&self, group_ids: Vec<u64>) -> Result<()> {
        self.groups
            .write()
            .await
            .retain(|group| !group_ids.contains(&group.id));

        storage::delete_groups(self.id, &group_ids).await?;
        for group_id in group_ids {
            match self.group_signals_sender.send(group_id) {
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

        let (stop_signal_tx, read_tx, write_tx) = self.run().await;
        self.stop_signal_tx = Some(stop_signal_tx);
        self.read_tx = Some(read_tx);
        self.write_tx = Some(write_tx);

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

        self.group_signals_sender.send(0);
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
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

    async fn update_group(&self, group_id: u64, req: &CreateGroupReq) -> Result<()> {
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
                    let _ = self.group_signals_sender.send(group_id);
                    self.run_group_timer(group_id, req.interval);
                }
            }
            None => bail!("组不存在"),
        };

        storage::update_group(self.id, group_id, serde_json::to_string(&req)?).await?;

        Ok(())
    }

    async fn create_points(
        &self,
        group_id: u64,
        create_points: Vec<(Option<u64>, CreatePointReq)>,
    ) -> Result<()> {
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

    async fn update_point(&self, group_id: u64, point_id: u64, req: &CreatePointReq) -> Result<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update_point(point_id, req).await,
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

async fn run_event_loop(
    mut ctx: Context,
    rtt: Arc<AtomicU16>,
    stop_signal: &mut mpsc::Receiver<()>,
    write_rx: &mut mpsc::Receiver<PointConf>,
    read_rx: &mut mpsc::Receiver<u64>,
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
                debug!("get {:?} data", group_id);
               if let Some(group_id) = group_id {
        if           !read_group_points(&mut ctx, &groups, group_id, interval).await {
           return

        }
               }
           }
           }
    }
    // }
}

async fn read_group_points(
    ctx: &mut Context,
    groups: &RwLock<Vec<Group>>,
    group_id: u64,
    interval: u64,
) -> bool {
    if let Some(group) = groups
        .write()
        .await
        .iter_mut()
        .find(|group| group.id == group_id)
    {
        for point in group.points.write().await.iter_mut() {
            ctx.set_slave(Slave(point.slave));
            // for rtt
            // let start_time = Instant::now();
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
                            return false;
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
                            return false;
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
                            return false;
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
                            return false;
                        }
                    },
                },
                _ => unreachable!(),
            }

            time::sleep(Duration::from_millis(interval)).await;
        }

        true
    } else {
        true
    }
}
