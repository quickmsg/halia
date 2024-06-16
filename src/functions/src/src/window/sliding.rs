use anyhow::Result;
use message::MessageBatch;
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    sync::broadcast::{Receiver, Sender},
    time,
};
use tracing::{debug, error};

pub struct Sliding {
    duration: u64,
    message_batch: MessageBatch,
    rx: Receiver<MessageBatch>,
    tx: Sender<MessageBatch>,
}

#[derive(Deserialize)]
pub struct WindowConf {
    duration: u64,
}

impl Sliding {
    pub fn new(
        conf: Value,
        rx: Receiver<MessageBatch>,
        tx: Sender<MessageBatch>,
    ) -> Result<Sliding> {
        let window_conf: WindowConf = serde_json::from_value(conf)?;

        Ok(Sliding {
            duration: window_conf.duration,
            message_batch: MessageBatch::default(),
            rx,
            tx,
        })
    }

    pub async fn run(&mut self) {
        debug!("window run");
        let mut interval = time::interval(time::Duration::from_secs(self.duration));
        debug!("duration:{:?}", self.duration);
        loop {
            tokio::select! {
               msg = self.rx.recv() => {
                match msg {
                    Ok(mb) => {
                        self.message_batch.merge(mb);
                    }
                    Err(_) => todo!(),
                }

               }
                //    立即返回。bug？？
               _ = interval.tick() => {
                debug!("tick");
                if !self.message_batch.is_empty(){
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
}
