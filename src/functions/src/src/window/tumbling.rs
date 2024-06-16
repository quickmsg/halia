use anyhow::Result;
use message::MessageBatch;
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    sync::broadcast::{Receiver, Sender},
    time,
};
use tracing::{debug, error};

pub struct Tumbling {
    duration: u64,
    message_batch: MessageBatch,
    rx: Receiver<MessageBatch>,
    tx: Sender<MessageBatch>,
}

#[derive(Deserialize)]
pub struct WindowConf {
    // must be non-zero
    duration: u64,
}

impl Tumbling {
    pub fn new(
        conf: Value,
        rx: Receiver<MessageBatch>,
        tx: Sender<MessageBatch>,
    ) -> Result<Tumbling> {
        let window_conf: WindowConf = serde_json::from_value(conf)?;

        Ok(Tumbling {
            duration: window_conf.duration,
            message_batch: MessageBatch::default(),
            rx,
            tx,
        })
    }

    pub async fn run(&mut self) {
        let mut interval = time::interval(time::Duration::from_secs(self.duration));
        // time::interval_at(start, period)
        loop {
            tokio::select! {
               msg = self.rx.recv() => {
                match msg {
                    Ok(mb) => {
                        self.message_batch.merge(mb);
                    }
                    Err(_) => debug!("upstream channel close")
                }

               }
                // 立即返回。bug？？
               _ = interval.tick() => {
                   debug!("tick");
                    match self.tx.send(self.message_batch.clone()) {
                        Ok(_) => { debug!("send msg success");},
                        Err(e) => error!("{}",e),
                    }
                    self.message_batch.clear();
               }
            }
        }
    }
}
