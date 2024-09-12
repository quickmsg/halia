use common::{
    error::HaliaResult,
    get_dynamic_value_from_json,
    sink_message_retain::{self, SinkMessageRetain},
};
use message::MessageBatch;
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tracing::{debug, warn};
use types::devices::modbus::SinkConf;

use super::WritePointEvent;

#[derive(Debug)]
pub struct Sink {
    stop_signal_tx: mpsc::Sender<()>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
            mpsc::Sender<WritePointEvent>,
            broadcast::Receiver<bool>,
            Box<dyn SinkMessageRetain>,
        )>,
    >,

    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub fn new(
        conf: SinkConf,
        write_tx: mpsc::Sender<WritePointEvent>,
        device_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let mut sink = Self {
            stop_signal_tx,
            join_handle: None,
            mb_tx,
        };

        let message_retainer = sink_message_retain::new(&conf.message_retain);

        sink.event_loop(
            stop_signal_rx,
            mb_rx,
            write_tx,
            conf,
            device_err_rx,
            message_retainer,
        );

        sink
    }

    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn update(&mut self, old_conf: String, new_conf: SinkConf) {
        let old_conf: SinkConf = serde_json::from_str(&old_conf).unwrap();
        if old_conf != new_conf {
            self.stop().await;

            let (stop_signal_rx, mb_rx, write_tx, device_err_rx, message_retainer) =
                self.join_handle.take().unwrap().await.unwrap();
            self.event_loop(
                stop_signal_rx,
                mb_rx,
                write_tx,
                new_conf,
                device_err_rx,
                message_retainer,
            );
        }
    }

    fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        write_tx: mpsc::Sender<WritePointEvent>,
        conf: SinkConf,
        mut device_err_rx: broadcast::Receiver<bool>,
        mut message_retainer: Box<dyn SinkMessageRetain>,
    ) {
        let join_handle = tokio::spawn(async move {
            let mut device_err = false;
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, mb_rx, write_tx, device_err_rx, message_retainer);
                    }

                    // todo 恢复时消息发送
                    mb = mb_rx.recv() => {
                        if let Some(mb) = mb {
                            if !device_err {
                                Self::send_write_point_event(mb, &conf, &write_tx).await;
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
                                            Self::send_write_point_event(mb, &conf, &write_tx).await;
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
        self.stop_signal_tx.send(()).await.unwrap();
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
