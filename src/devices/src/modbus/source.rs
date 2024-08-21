use std::{io, sync::Arc, time::Duration};

use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::{
    del_mb_rx,
    error::{HaliaError, HaliaResult},
    get_id, get_mb_rx, persistence,
    ref_info::RefInfo,
};
use message::{Message, MessageBatch};
use protocol::modbus::Context;
use serde_json::Value;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::{debug, warn};
use types::{
    devices::modbus::{Area, SourceConf},
    BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksItemResp,
};
use uuid::Uuid;

#[derive(Debug)]
pub struct Source {
    pub id: Uuid,

    pub base_conf: BaseConf,
    pub ext_conf: SourceConf,
    quantity: u16,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Sender<Uuid>,
            Arc<RwLock<Option<String>>>,
        )>,
    >,
    value: serde_json::Value,
    err_info: Option<String>,

    pub ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Source {
    pub async fn new(
        device_id: &Uuid,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<Self> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;

        let (source_id, new) = get_id(source_id);
        if new {
            persistence::create_source(device_id, &source_id, &data).await?;
        }

        let quantity = ext_conf.data_type.get_quantity();

        Ok(Self {
            id: source_id,
            base_conf,
            ext_conf,
            quantity,
            value: Value::Null,
            stop_signal_tx: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
            join_handle: None,
            err_info: None,
        })
    }

    fn parse_conf(req: CreateUpdateSourceOrSinkReq) -> HaliaResult<(BaseConf, SourceConf, String)> {
        let data = serde_json::to_string(&req)?;
        let conf: SourceConf = serde_json::from_value(req.ext)?;

        if conf.interval == 0 {
            return Err(HaliaError::Common("点位频率必须大于0".to_owned()));
        }

        // TODO 其他检查
        Ok((req.base, conf, data))
    }

    // pub fn check_duplicate(&self, req: &CreateUpdatePointReq) -> HaliaResult<()> {
    //     if self.conf.base.name == req.base.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     if self.conf.ext.data_type == req.ext.data_type
    //         && self.conf.ext.slave == req.ext.slave
    //         && self.conf.ext.area == req.ext.area
    //         && self.conf.ext.address == req.ext.address
    //     {
    //         return Err(HaliaError::AddressExists);
    //     }

    //     Ok(())
    // }

    pub fn search(&self) -> SearchSourcesOrSinksItemResp {
        SearchSourcesOrSinksItemResp {
            id: self.id.clone(),
            conf: CreateUpdateSourceOrSinkReq {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(&self.ext_conf).unwrap(),
            },
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn start(
        &mut self,
        read_tx: mpsc::Sender<Uuid>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, _) = broadcast::channel(16);
        self.mb_tx = Some(mb_tx);

        self.event_loop(self.ext_conf.interval, stop_signal_rx, read_tx, device_err)
            .await;
    }

    async fn event_loop(
        &mut self,
        interval: u64,
        mut stop_signal_rx: mpsc::Receiver<()>,
        read_tx: mpsc::Sender<Uuid>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let point_id = self.id.clone();
        let join_handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, read_tx, device_err);
                    }

                    _ = interval.tick() => {
                        if device_err.read().await.is_none() {
                            if let Err(e) = read_tx.send(point_id).await {
                                debug!("send point info err :{}", e);
                            }
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;

        persistence::update_source(device_id, &self.id, &data).await?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.quantity = ext_conf.data_type.get_quantity();
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        if self.stop_signal_tx.is_some() && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (stop_signal_rx, read_tx, device_err) =
                self.join_handle.take().unwrap().await.unwrap();
            self.event_loop(self.ext_conf.interval, stop_signal_rx, read_tx, device_err)
                .await;
        }

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::DeleteRefing);
        }

        self.stop().await;
        persistence::delete_source(device_id, &self.id).await?;
        Ok(())
    }

    pub async fn read(&mut self, ctx: &mut Box<dyn Context>) -> io::Result<()> {
        let res = match self.ext_conf.area {
            Area::InputDiscrete => {
                ctx.read_discrete_inputs(self.ext_conf.slave, self.ext_conf.address, self.quantity)
                    .await
            }
            Area::Coils => {
                ctx.read_coils(self.ext_conf.slave, self.ext_conf.address, self.quantity)
                    .await
            }
            Area::InputRegisters => {
                ctx.read_input_registers(self.ext_conf.slave, self.ext_conf.address, self.quantity)
                    .await
            }
            Area::HoldingRegisters => {
                ctx.read_holding_registers(
                    self.ext_conf.slave,
                    self.ext_conf.address,
                    self.quantity,
                )
                .await
            }
        };

        match res {
            Ok(mut data) => {
                let value = self.ext_conf.data_type.decode(&mut data);
                match &value {
                    message::MessageValue::Bytes(bytes) => {
                        let str = BASE64_STANDARD.encode(bytes);
                        self.value = serde_json::Value::String(str);
                    }
                    _ => self.value = value.clone().into(),
                }

                match &self.mb_tx {
                    Some(tx) => {
                        if tx.receiver_count() == 0 {
                            return Ok(());
                        }
                        let mut message = Message::default();
                        message.add(self.base_conf.name.clone(), value);
                        let mut message_batch = MessageBatch::default();
                        message_batch.push_message(message);
                        if let Err(e) = tx.send(message_batch) {
                            warn!("send err :{:?}", e);
                        }
                    }
                    None => {}
                }
                Ok(())
            }
            Err(e) => match e {
                protocol::modbus::ModbusError::Transport(e) => Err(e),
                protocol::modbus::ModbusError::Protocol(e) => {
                    warn!("{}", e);
                    Ok(())
                }
                protocol::modbus::ModbusError::Exception(e) => {
                    warn!("{}", e);
                    Ok(())
                }
            },
        }
    }

    pub fn get_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        get_mb_rx!(self, rule_id)
    }

    pub fn del_rx(&mut self, rule_id: &Uuid) {
        del_mb_rx!(self, rule_id)
    }
}
