use common::{
    error::HaliaResult,
    get_dynamic_value_from_json,
    sink_message_retain::{self, SinkMessageRetain},
};
use message::{MessageBatch, RuleMessageBatch};
use tokio::{
    select,
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};
use tracing::{debug, warn};
use types::devices::device::modbus::SinkConf;

use super::WritePointEvent;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: mpsc::UnboundedSender<RuleMessageBatch>,
}

pub struct JoinHandleData {
    pub conf: SinkConf,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: mpsc::UnboundedReceiver<RuleMessageBatch>,
    pub write_tx: mpsc::Sender<WritePointEvent>,
    pub device_err_rx: broadcast::Receiver<bool>,
    pub message_retainer: Box<dyn SinkMessageRetain>,
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn new(
        conf: SinkConf,
        write_tx: mpsc::Sender<WritePointEvent>,
        device_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = mpsc::unbounded_channel();

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let join_handle_data = JoinHandleData {
            conf,
            stop_signal_rx,
            mb_rx,
            write_tx,
            device_err_rx,
            message_retainer,
        };

        let join_handle = Self::event_loop(join_handle_data);

        Self {
            stop_signal_tx,
            join_handle: Some(join_handle),
            mb_tx,
        }
    }

    pub async fn update(&mut self, conf: SinkConf) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = conf;
        let join_handle = Self::event_loop(join_handle_data);
        self.join_handle = Some(join_handle);
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            let mut device_err = false;
            debug!("here");
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    mb = join_handle_data.mb_rx.recv() => {
                        if let Some(mb) = mb {
                            let mb = mb.take_mb();
                            if !device_err {
                                Self::send_write_point_event(mb, &join_handle_data.conf, &join_handle_data.write_tx).await;
                            } else {
                                join_handle_data.message_retainer.push(mb);
                            }
                        }
                    }

                    err = join_handle_data.device_err_rx.recv() => {
                        match err {
                            Ok(err) =>{
                                device_err = err;
                                match err {
                                    true => {
                                        while let Some(mb) = join_handle_data.message_retainer.pop() {
                                            Self::send_write_point_event(mb, &join_handle_data.conf, &join_handle_data.write_tx).await;
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
        })
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
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

        let value = match get_dynamic_value_from_json(&sink_conf.value) {
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