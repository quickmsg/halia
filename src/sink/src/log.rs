use anyhow::Result;
use common::error::HaliaResult;
use message::MessageBatch;
use tokio::sync::broadcast::Receiver;
use tracing::{debug, info};
use types::{
    rule::Status,
    sink::{CreateSinkReq, ListSinkResp},
};
use uuid::Uuid;

use crate::Sink;

pub struct Log {
    id: Uuid,
    name: String,
    status: Status,
}

impl Log {
    pub fn new(id: Uuid, req: &CreateSinkReq) -> HaliaResult<Box<dyn Sink>> {
        Ok(Box::new(Log {
            id,
            status: Status::Stopped,
            name: req.name.clone(),
        }))
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

    fn get_detail(&self) -> Result<types::sink::ReadSinkResp> {
        Ok(types::sink::ReadSinkResp {
            r#type: "log".to_string(),
            name: self.name.clone(),
            conf: serde_json::Value::Null,
        })
    }

    fn get_info(&self) -> Result<types::sink::ListSinkResp> {
        Ok(ListSinkResp {
            id: self.id.clone(),
            name: self.name.clone(),
            r#type: "log".to_string(),
        })
    }
}
