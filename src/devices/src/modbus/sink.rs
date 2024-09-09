use common::{
    error::{HaliaError, HaliaResult},
    get_dynamic_value_from_json, get_search_sources_or_sinks_info_resp,
    sink_message_retain::{self, SinkMessageRetain},
};
use message::MessageBatch;
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tracing::{debug, warn};
use types::{
    devices::modbus::SinkConf, BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp,
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
            broadcast::Receiver<bool>,
            Box<dyn SinkMessageRetain>,
        )>,
    >,

    pub mb_tx: Option<mpsc::Sender<MessageBatch>>,
}

impl Sink {
    pub fn new(sink_id: Uuid, base_conf: BaseConf, ext_conf: SinkConf) -> Self {
        Sink {
            id: sink_id,
            base_conf,
            ext_conf,
            stop_signal_tx: None,
            join_handle: None,
            mb_tx: None,
        }
    }

    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, _ext_conf: &SinkConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self)
    }

    pub async fn update(&mut self, base_conf: BaseConf, ext_conf: SinkConf) {
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

                    let (stop_signal_rx, publish_rx, tx, device_err_rx, message_retainer) =
                        self.join_handle.take().unwrap().await.unwrap();
                    self.event_loop(
                        stop_signal_rx,
                        publish_rx,
                        tx,
                        self.ext_conf.clone(),
                        device_err_rx,
                        message_retainer,
                    )
                    .await;
                }
                None => {}
            }
        }
    }

    pub async fn start(
        &mut self,
        device_tx: mpsc::Sender<WritePointEvent>,
        device_err_rx: broadcast::Receiver<bool>,
    ) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(mb_tx);

        let message_retainer = sink_message_retain::new(&self.ext_conf.message_retain);

        self.event_loop(
            stop_signal_rx,
            mb_rx,
            device_tx,
            self.ext_conf.clone(),
            device_err_rx,
            message_retainer,
        )
        .await;
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        device_tx: mpsc::Sender<WritePointEvent>,
        sink_conf: SinkConf,
        mut device_err_rx: broadcast::Receiver<bool>,
        mut message_retainer: Box<dyn SinkMessageRetain>,
    ) {
        let join_handle = tokio::spawn(async move {
            let mut device_err = false;
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, mb_rx, device_tx, device_err_rx, message_retainer);
                    }

                    // todo 恢复时消息发送
                    mb = mb_rx.recv() => {
                        if let Some(mb) = mb {
                            if !device_err {
                                Self::send_write_point_event(mb, &sink_conf, &device_tx).await;
                            } else {
                                message_retainer.push(mb);
                            }
                        }
                    }

                    err = device_err_rx.recv() => {
                        match err {
                            Ok(err) =>{
                                device_err = err;
                                match err {
                                    true => {
                                        while let Some(mb) = message_retainer.pop() {
                                            Self::send_write_point_event(mb, &sink_conf, &device_tx).await;
                                        }
                                    }
                                    false => {}
                                }
                            }
                            Err(e) => warn!("{}", e),
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
