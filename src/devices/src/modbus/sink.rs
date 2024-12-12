use common::{
    error::HaliaResult,
    get_dynamic_value_from_json,
    sink_message_retain::{self, SinkMessageRetain},
};
use message::{MessageBatch, RuleMessageBatch};
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{self, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tracing::debug;
use types::devices::device::modbus::SinkConf;

use super::WritePointEvent;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<TaskLoop>>,
    pub mb_tx: mpsc::UnboundedSender<RuleMessageBatch>,
}

pub struct TaskLoop {
    sink_conf: SinkConf,
    stop_signal_rx: watch::Receiver<()>,
    mb_rx: mpsc::UnboundedReceiver<RuleMessageBatch>,
    write_tx: UnboundedSender<WritePointEvent>,
    device_err_rx: broadcast::Receiver<bool>,
    message_retainer: Box<dyn SinkMessageRetain>,
}

impl TaskLoop {
    fn new(
        sink_conf: SinkConf,
        stop_signal_rx: watch::Receiver<()>,
        mb_rx: mpsc::UnboundedReceiver<RuleMessageBatch>,
        write_tx: UnboundedSender<WritePointEvent>,
        device_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let message_retainer = sink_message_retain::new(&sink_conf.message_retain);
        Self {
            sink_conf,
            stop_signal_rx,
            mb_rx,
            write_tx,
            device_err_rx,
            message_retainer,
        }
    }

    fn start(mut self) -> JoinHandle<Self> {
        tokio::spawn(async move {
            let mut device_err = false;
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    Some(mb) = self.mb_rx.recv() => {
                        let mb = mb.take_mb();
                        if !device_err {
                            self.send_write_point_event(mb).await;
                        } else {
                            self.message_retainer.push(mb);
                        }
                    }

                    Ok(err) = self.device_err_rx.recv() => {
                        device_err = err;
                        match err {
                            true => {
                                while let Some(mb) = self.message_retainer.pop() {
                                    self.send_write_point_event(mb).await;
                                }
                            }
                            false => {}
                        }
                    }
                }
            }
        })
    }

    async fn send_write_point_event(&self, mut mb: MessageBatch) {
        let message = match mb.take_one_message() {
            Some(message) => message,
            None => return,
        };

        let value = match get_dynamic_value_from_json(&self.sink_conf.value) {
            common::DynamicValue::Const(value) => value,
            common::DynamicValue::Field(s) => match message.get(&s) {
                Some(v) => v.clone().into(),
                None => return,
            },
        };

        match WritePointEvent::new(
            self.sink_conf.slave,
            self.sink_conf.area,
            self.sink_conf.address,
            self.sink_conf.data_type,
            value,
        ) {
            Ok(wpe) => {
                _ = self.write_tx.send(wpe);
            }
            Err(e) => {
                debug!("value is err :{e}");
            }
        }
    }
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn new(
        sink_conf: SinkConf,
        write_tx: UnboundedSender<WritePointEvent>,
        device_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = mpsc::unbounded_channel();

        let task_loop = TaskLoop::new(sink_conf, stop_signal_rx, mb_rx, write_tx, device_err_rx);
        let join_handle = task_loop.start();

        Self {
            stop_signal_tx,
            join_handle: Some(join_handle),
            mb_tx,
        }
    }

    pub async fn update(&mut self, sink_conf: serde_json::Value) -> HaliaResult<()> {
        let mut task_loop = self.stop().await;
        let sink_conf: SinkConf = serde_json::from_value(sink_conf)?;
        task_loop.sink_conf = sink_conf;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
        Ok(())
    }

    pub async fn stop(&mut self) -> TaskLoop {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub fn get_txs(&self, cnt: usize) -> Vec<UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];
        for _ in 0..cnt {
            txs.push(self.mb_tx.clone());
        }
        txs
    }
}