use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    get_dynamic_value_from_json, get_id, persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use tokio::{
    select,
    sync::{mpsc, RwLock},
    task::JoinHandle,
};
use tracing::debug;
use types::{
    devices::modbus::SinkConf, BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksItemResp,
};
use uuid::Uuid;

use super::WritePointEvent;

#[derive(Debug)]
pub struct Sink {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: SinkConf,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
            mpsc::Sender<WritePointEvent>,
            Arc<RwLock<Option<String>>>,
        )>,
    >,

    pub ref_info: RefInfo,
    mb_tx: Option<mpsc::Sender<MessageBatch>>,
}

impl Sink {
    pub async fn new(
        device_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<Self> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;

        let (sink_id, new) = get_id(sink_id);
        if new {
            persistence::create_sink(device_id, &sink_id, &data).await?;
        }

        Ok(Sink {
            id: sink_id,
            base_conf,
            ext_conf,
            stop_signal_tx: None,
            join_handle: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
        })
    }

    fn parse_conf(req: CreateUpdateSourceOrSinkReq) -> HaliaResult<(BaseConf, SinkConf, String)> {
        let data = serde_json::to_string(&req)?;
        let conf: SinkConf = serde_json::from_value(req.ext)?;

        Ok((req.base, conf, data))
    }

    // pub fn check_duplicate(&self, req: &CreateUpdateSinkReq) -> HaliaResult<()> {
    //     if self.conf.base.name == req.base.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     Ok(())
    // }

    pub fn search(&self) -> SearchSourcesOrSinksItemResp {
        SearchSourcesOrSinksItemResp {
            id: self.id.clone(),
            conf: CreateUpdateSourceOrSinkReq {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(self.ext_conf.clone()).unwrap(),
            },
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;

        persistence::update_sink(device_id, &self.id, &data).await?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        if restart {
            match &self.stop_signal_tx {
                Some(stop_signal_tx) => {
                    stop_signal_tx.send(()).await.unwrap();

                    let (stop_signal_rx, publish_rx, tx, device_err) =
                        self.join_handle.take().unwrap().await.unwrap();
                    self.event_loop(
                        stop_signal_rx,
                        publish_rx,
                        tx,
                        self.ext_conf.clone(),
                        device_err,
                    )
                    .await;
                }
                None => {}
            }
        }
        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::Common("该动作含有引用规则".to_owned()));
        }

        persistence::devices::modbus::delete_sink(device_id, &self.id).await?;
        match self.stop_signal_tx {
            Some(_) => self.stop().await,
            None => {}
        }

        Ok(())
    }

    pub fn get_tx(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.mb_tx.as_ref().unwrap().clone()
    }

    pub async fn start(
        &mut self,
        device_tx: mpsc::Sender<WritePointEvent>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(mb_tx);

        self.event_loop(
            stop_signal_rx,
            mb_rx,
            device_tx,
            self.ext_conf.clone(),
            device_err,
        )
        .await;
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        device_tx: mpsc::Sender<WritePointEvent>,
        sink_conf: SinkConf,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, mb_rx, device_tx, device_err);
                    }

                    mb = mb_rx.recv() => {
                        if let Some(mb) = mb {
                            if device_err.read().await.is_none() {
                                Sink::send_write_point_event(mb, &sink_conf, &device_tx).await;
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

    async fn send_write_point_event(
        mut mb: MessageBatch,
        sink_conf: &SinkConf,
        tx: &mpsc::Sender<WritePointEvent>,
    ) {
        let message = match mb.take_one_message() {
            Some(message) => message,
            None => return,
        };

        let value = match get_dynamic_value_from_json(sink_conf.value.clone()) {
            common::DynamicValue::Const(value) => value,
            common::DynamicValue::Field(s) => match message.get(&s) {
                Some(v) => v.clone().into(),
                None => return,
            },
        };
        debug!("{:?}", value);

        match WritePointEvent::new(
            sink_conf.slave,
            sink_conf.area,
            sink_conf.address,
            sink_conf.data_type,
            value,
        ) {
            Ok(wpe) => {
                tx.send(wpe).await.unwrap();
            }
            Err(e) => {
                debug!("value is err :{e}");
            }
        }
    }
}
