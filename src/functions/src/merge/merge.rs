use std::sync::Arc;

use anyhow::Result;
use message::{Message, MessageBatch};
use serde_json::Value;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    RwLock,
};
use tracing::{debug, error};

pub struct Merge {
    rxs: Vec<Receiver<MessageBatch>>,
    tx: Sender<MessageBatch>,
}

impl Merge {
    pub fn new(rxs: Vec<Receiver<MessageBatch>>, tx: Sender<MessageBatch>) -> Result<Merge> {
        Ok(Merge { rxs, tx })
    }

    pub async fn run(&mut self) {
        debug!("merge run");
        let messages: Arc<RwLock<Vec<(String, Option<Message>)>>> =
            Arc::new(RwLock::new(Vec::with_capacity(self.rxs.len())));
        for name in &self.names {
            messages.write().await.push((name.clone(), None));
        }

        while let Some(mut rx) = self.rxs.pop() {
            let messages_clone = messages.clone();
            let tx = self.tx.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(mut mb) => match mb.take_one_message() {
                            Some(message) => {
                                match messages_clone
                                    .write()
                                    .await
                                    .iter_mut()
                                    .find(|m| m.0 == *mb.get_name())
                                {
                                    Some(m) => m.1 = Some(message),
                                    None => todo!(),
                                }

                                let mut ready = true;
                                for (_, mb) in messages_clone.read().await.iter() {
                                    if mb.is_none() {
                                        ready = false;
                                        break;
                                    }
                                }

                                if ready {
                                    let mut mb = MessageBatch::default();
                                    let mut merge_message = Message::default();
                                    for (_, message) in messages_clone.write().await.iter_mut() {
                                        merge_message.merge(message.take().unwrap());
                                    }
                                    mb.push_message(merge_message);
                                    tx.send(mb).unwrap();
                                }
                            }
                            None => {}
                        },
                        Err(_) => todo!(),
                    }
                }
            });
        }

        // loop {
        //     let futures: Vec<_> = rxs.iter_mut().map(|rx| rx.recv()).collect();
        //     pin_mut!(futures);
        //     let (message_batch, _index, _remaing) = select_all(futures).await;
        //     match message_batch {
        //         Ok(mut message_batch) => {
        //             debug!("{:?}", message_batch);
        // let message = message_batch.get_one_message();
        // let message_ts = message.get_i64("_ts");
        // match message_ts {
        //     Some(message_ts) => {
        //         if message_ts > self.latest {
        //             self.latest = message_ts;
        //         }
        //     }
        //     None => error!("not have _ts"),
        // }
        // self.messages
        //     .insert(message_batch.get_name().to_string(), message);

        // self.messages.retain(|_, value| {
        //     let _ts = value.get_i64("_ts");
        //     match _ts {
        //         Some(_ts) => {
        //             if self.latest - _ts > self.duration {
        //                 return false;
        //             }
        //         }
        //         None => unreachable!(),
        //     }
        //     true
        // });
        // if self.messages.len() == self.count {
        //     let message = Message::merge(&self.messages);
        //     match self.tx.send(MessageBatch::from_message(message)) {
        //         Ok(_) => {}
        //         Err(e) => error!("send err:{}", e),
        //     }
        //     self.messages.clear();
        // }
    }
    //     Err(err) => {
    //         error!("Error receiving message: {:?}", err);
    //         return;
    //         // Handle the error as needed
    //     }
    // }
}
