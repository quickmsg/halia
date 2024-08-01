use std::sync::Arc;

use anyhow::Result;
use message::{Message, MessageBatch};
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
        let messages: Arc<RwLock<Vec<Option<Message>>>> =
            Arc::new(RwLock::new(Vec::with_capacity(self.rxs.len())));

        for _ in 0..self.rxs.len() {
            messages.write().await.push(None);
        }

        let mut i = 0;
        while let Some(mut rx) = self.rxs.pop() {
            let messages_clone = messages.clone();
            let tx = self.tx.clone();
            let pos = i;
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(mut mb) => match mb.take_one_message() {
                            Some(coming_message) => {
                                (messages_clone.write().await)[pos] = Some(coming_message);

                                let mut ready = true;
                                for message in messages_clone.read().await.iter() {
                                    if message.is_none() {
                                        ready = false;
                                        break;
                                    }
                                }

                                if ready {
                                    let mut mb = MessageBatch::default();
                                    let mut merge_message = Message::default();
                                    for message in messages_clone.write().await.iter_mut() {
                                        merge_message.merge(message.take().unwrap());
                                    }
                                    mb.push_message(merge_message);
                                    if let Err(e) = tx.send(mb) {
                                        debug!("{:?}", e);
                                    }
                                }
                            }
                            None => {}
                        },
                        Err(_) => todo!(),
                    }
                }
            });
            i += 1;
        }
    }
}
