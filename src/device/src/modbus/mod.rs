use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use group::Group;
use message::MessageBatch;
use protocol::modbus::client::{rtu, tcp, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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
use tracing::{debug, error};
use types::device::{
    device::{CreateDeviceReq, Mode, SearchDeviceItemResp, SearchSinksResp, UpdateDeviceReq},
    group::{CreateGroupReq, SearchGroupItemResp, SearchGroupResp, UpdateGroupReq},
    point::{CreatePointReq, SearchPointResp, WritePointValueReq},
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
    write_tx: Option<mpsc::Sender<(Uuid, Uuid, Value)>>,
    ref_cnt: usize,

    sinks: Vec<Sink>,
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

        sinks: vec![],
    }))
}

impl Modbus {
    async fn run(
        &self,
        mut signal_rx: mpsc::Receiver<Command>,
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
}

#[async_trait]
impl Device for Modbus {
    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()> {
        let update_conf: Conf = serde_json::from_value(req.conf.clone())?;

        if self.name != req.name {
            self.name = req.name.clone();
        }

        *self.conf.lock().await = update_conf.clone();
        if self.on.load(Ordering::SeqCst) {
            let _ = self.signal_tx.as_ref().unwrap().send(Command::Update);
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

    async fn create_group(
        &mut self,
        group_id: Option<Uuid>,
        req: &CreateGroupReq,
    ) -> HaliaResult<()> {
        let (group_id, create) = match group_id {
            Some(group_id) => (group_id, false),
            None => (Uuid::new_v4(), true),
        };

        if create {
            if let Err(e) =
                persistence::group::insert(&self.id, &group_id, &serde_json::to_string(&req)?).await
            {
                error!("wirte group to file err:{}", e);
            }
        }

        match Group::new(self.id, group_id, &req) {
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

        persistence::group::delete(&self.id, &group_id).await?;
        match self
            .group_signal_tx
            .as_ref()
            .unwrap()
            .send(group::Command::Stop(group_id))
        {
            Ok(_) => {}
            Err(e) => error!("group send stop singla err:{}", e),
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

        let (write_tx, write_rx) = mpsc::channel::<(Uuid, Uuid, Value)>(10);
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

    async fn read_groups(&self, page: usize, size: usize) -> HaliaResult<SearchGroupResp> {
        let mut resps = Vec::new();
        for group in self
            .groups
            .read()
            .await
            .iter()
            .skip(((page - 1) * size) as usize)
        {
            resps.push({
                SearchGroupItemResp {
                    id: group.id,
                    name: group.name.clone(),
                    interval: group.interval,
                    point_count: group.get_points_num().await as u8,
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

    async fn update_group(&self, group_id: Uuid, req: &UpdateGroupReq) -> HaliaResult<()> {
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

        persistence::group::update(&self.id, &group_id, &serde_json::to_string(&req)?).await?;

        Ok(())
    }

    async fn create_point(
        &self,
        group_id: Uuid,
        point_id: Option<Uuid>,
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
            Some(group) => Ok(group.search_point(page, size).await),
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
        if self.on.load(Ordering::SeqCst) == false {
            return Err(HaliaError::DeviceStoped);
        }
        if self.err.load(Ordering::SeqCst) == true {
            return Err(HaliaError::DeviceDisconnect);
        }

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

    async fn subscribe(
        &mut self,
        group_id: Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        self.ref_cnt += 1;
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

    async fn create_sink(&mut self, req: Bytes) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_slice(&req)?;
        let sink = sink::new(Uuid::new_v4(), conf)?;
        // TODO
        sink.run();
        self.sinks.push(sink);
        Ok(())
    }

    async fn search_sinks(&mut self, page: usize, size: usize) -> SearchSinksResp {
        let mut data = vec![];
        let mut i = 0;
        for sink in self.sinks.iter().skip((page - 1) * size) {
            data.push(sink.search());
            i += 1;
            if i >= size {
                break;
            }
        }
        SearchSinksResp {
            total: self.sinks.len(),
            data,
        }
    }

    async fn update_sink(&mut self) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_sink(&mut self) -> HaliaResult<()> {
        todo!()
    }
}

async fn run_event_loop(
    mut ctx: Context,
    rtt: &Arc<AtomicU16>,
    signal_rx: &mut mpsc::Receiver<Command>,
    write_rx: &mut mpsc::Receiver<(Uuid, Uuid, Value)>,
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

            point = write_rx.recv() => {
                if let Some(point) = point {
                    if !write_point_value(&mut ctx, &groups, point.0, point.1, point.2, interval, rtt).await {
                        return
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

async fn write_point_value(
    ctx: &mut Context,
    groups: &RwLock<Vec<Group>>,
    group_id: Uuid,
    point_id: Uuid,
    value: Value,
    interval: u64,
    rtt: &Arc<AtomicU16>,
) -> bool {
    if let Some(group) = groups
        .read()
        .await
        .iter()
        .find(|group| group.id == group_id)
    {
        group.write(ctx, interval, point_id, value, rtt).await;
        return true;
    }

    true
}
