use anyhow::Result;
use message::MessageBatch;
use tokio::sync::broadcast::Receiver;
use tracing::{debug, info};
use types::rule::Status;

use crate::Sink;

pub struct Log {
    status: Status,
}

impl Log {
    pub fn new() -> Result<Log> {
        Ok(Log {
            status: Status::Stopped,
        })
    }
}

async fn run(mut rx: Receiver<MessageBatch>) {
    debug!("log sink run");
    loop {
        match rx.recv().await {
            Ok(msg) => info!("log receive msg: {:?}", msg),
            Err(e) => {
                debug!("err:{:?}", e);
                return;
            }
        }
    }
}

impl Sink for Log {
    fn insert_receiver(&mut self, rx: Receiver<MessageBatch>) -> Result<()> {
        match self.status {
            Status::Stopped => {
                tokio::spawn(async move {
                    run(rx).await;
                });
                Ok(())
            } // Status::Running => match &self.tx {
            //     Some(tx) => {
            //         return Ok(tx.clone());
            //     }
            //     None => {
            //         bail!("tx is None");
            //     }
            // },

            //   TODO
            Status::Running => Ok(()),
        }
    }
}
