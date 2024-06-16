use anyhow::anyhow;
use anyhow::Result;
use message::Message;
use serde_json::Value;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::debug;

pub struct Join {
    rxs: Vec<Receiver<Message>>,
    tx: Sender<Message>,
}

impl Join {
    pub fn new(
        conf: Option<Value>,
        rxs: Vec<Receiver<Message>>,
        tx: Sender<Message>,
    ) -> Result<Join> {
        let conf = conf.ok_or_else(|| anyhow!("conf is required"))?;
        Ok(Join { rxs, tx })
    }

    pub async fn run(&mut self) {
        debug!("join run");
        loop {
            for (index, rx) in &mut self.rxs.iter_mut().enumerate() {
                tokio::select! {
                         msg = rx.recv() => {
                             if let Ok(msg) = msg {
                                 debug!("get msg:{:?} from {:?}", msg, index);
                                //  self.messages[index] = msg;

                                //   let mut out_msg = Message::empty();
                                  // todo
                                //   for msg_opt in &self.messages {
                                      // for field in &msg.fields() {


                                      // }
                                  // }
                                //   }
                                // match  self.tx.send(out_msg) {
                                // Ok(_)=>{}
                    //  Err(e) => {
                    //   error!("{:?}", e);
                    //  }
                                // }
                                //  self.messages.push(msg);
                             } else {
                                      // debug!("error receiving msg");
                             }
                         }
                }
            }
        }
    }
}
