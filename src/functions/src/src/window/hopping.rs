use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use message::MessageBatch;
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    sync::broadcast::{Receiver, Sender},
    time,
};
use tracing::{debug, error};

pub struct Hopping {
    duration: u64,
    hopping_duration: u64,
    message_batchs: Vec<(u64, MessageBatch)>,
    rx: Receiver<MessageBatch>,
    tx: Sender<MessageBatch>,
}

#[derive(Deserialize)]
pub struct WindowConf {
    duration: u64,
    hopping_duration: u64,
}

impl Hopping {
    pub fn new(
        conf: Value,
        rx: Receiver<MessageBatch>,
        tx: Sender<MessageBatch>,
    ) -> Result<Hopping> {
        let window_conf: WindowConf = serde_json::from_value(conf)?;

        Ok(Hopping {
            duration: window_conf.duration,
            hopping_duration: window_conf.hopping_duration,
            message_batchs: Vec::new(),
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
                        let time = SystemTime::now().duration_since(UNIX_EPOCH);
                        match time {
                            Ok(duration) => {
                                self.message_batchs.push((duration.as_secs(), mb));
                            }
                            Err(_) => {}
                        }

                    }
                    Err(_) => todo!(),
                }
               }
                //    立即返回。bug？？
               instant = interval.tick() => {

                if !self.message_batchs.is_empty() {
                    let time = SystemTime::now().duration_since(UNIX_EPOCH);
                    match time {
                        Ok(duration) => {
                            let now_ts = duration.as_secs();
                            let mut mb = MessageBatch::default();
                            mb.with_name(self.message_batchs[0].1.get_name().to_string());
                            // TODO 性能优化 删除过期的
                            for (timestamp, message_batch) in &self.message_batchs {
                                mb.merge(message_batch.clone());
                                if now_ts - timestamp > self.hopping_duration {
                                    // self.message_batchs.remove()
                                }
                            }
                        }
                        Err(_) => todo!(),
                    }

                }
                    debug!("tick");
                    // match self.tx.send(self.message_batch.clone()) {
                    //     Ok(_) => { debug!("send msg success");},
                    //     Err(e) => error!("{}",e),
                    // }
               }
            }
        }
    }
}
