use common::{
    error::HaliaResult,
    get_dynamic_value_from_json,
    sink_message_retain::{self, SinkMessageRetain},
};
use message::MessageBatch;
use tokio::{
    select,
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};
use tracing::{debug, warn};
use types::devices::modbus::SinkConf;

use super::WritePointEvent;

#[derive(Debug)]
pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<
        JoinHandle<(
            watch::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
            mpsc::Sender<WritePointEvent>,
            broadcast::Receiver<bool>,
            Box<dyn SinkMessageRetain>,
        )>,
    >,

    pub mb_tx: mpsc::Sender<MessageBatch>,
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
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let join_handle = Self::event_loop(
            stop_signal_rx,
            mb_rx,
            write_tx,
            conf,
            device_err_rx,
            message_retainer,
        );

        Self {
            stop_signal_tx,
            join_handle: Some(join_handle),
            mb_tx,
        }
    }

    pub async fn update(&mut self, _old_conf: SinkConf, new_conf: SinkConf) {
        let (stop_signal_rx, mb_rx, write_tx, device_err_rx, message_retainer) = self.stop().await;
        let join_handle = Self::event_loop(
            stop_signal_rx,
            mb_rx,
            write_tx,
            new_conf,
            device_err_rx,
            message_retainer,
        );
        self.join_handle = Some(join_handle);
    }

    fn event_loop(
        mut stop_signal_rx: watch::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        write_tx: mpsc::Sender<WritePointEvent>,
        conf: SinkConf,
        mut device_err_rx: broadcast::Receiver<bool>,
        mut message_retainer: Box<dyn SinkMessageRetain>,
    ) -> JoinHandle<(
        watch::Receiver<()>,
        mpsc::Receiver<MessageBatch>,
        mpsc::Sender<WritePointEvent>,
        broadcast::Receiver<bool>,
        Box<dyn SinkMessageRetain>,
    )> {
        tokio::spawn(async move {
            let mut device_err = false;
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return (stop_signal_rx, mb_rx, write_tx, device_err_rx, message_retainer);
                    }

                    mb = mb_rx.recv() => {
                        debug!("{:?}", mb);
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
        })
    }

    pub async fn stop(
        &mut self,
    ) -> (
        watch::Receiver<()>,
        mpsc::Receiver<MessageBatch>,
        mpsc::Sender<WritePointEvent>,
        broadcast::Receiver<bool>,
        Box<dyn SinkMessageRetain>,
    ) {
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
