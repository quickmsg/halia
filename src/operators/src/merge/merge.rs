use anyhow::Result;
use futures::{future::select_all, FutureExt};
use indexmap::IndexMap;
use message::{Message, MessageBatch};
use serde_json::Value;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{debug, error};

pub struct Merge {
    rxs: Vec<Receiver<MessageBatch>>,
    tx: Sender<MessageBatch>,
    messages: IndexMap<String, Message>,
    duration: i64,
    count: usize,
    latest: i64,
}

impl Merge {
    pub fn new(
        conf: Value,
        rxs: Vec<Receiver<MessageBatch>>,
        tx: Sender<MessageBatch>,
    ) -> Result<Merge> {
        let duration: i64 = serde_json::from_value(conf)?;

        Ok(Merge {
            count: rxs.len(),
            messages: IndexMap::with_capacity(rxs.len()),
            rxs,
            duration,
            tx,
            latest: -1,
        })
    }

    pub async fn run(&mut self) {
        debug!("merge run");
        loop {
            let mut receiver_streams = Vec::new();
            for rx in &mut self.rxs {
                receiver_streams.push(Box::pin(rx.recv().fuse()));
            }
            let (message_batch, _index, remaining_streams) = select_all(receiver_streams).await;
            match message_batch {
                Ok(mut message_batch) => {
                    debug!("{:?}", message_batch);
                    let message = message_batch.get_one_message();
                    let message_ts = message.get_i64("_ts");
                    match message_ts {
                        Some(message_ts) => {
                            if message_ts > self.latest {
                                self.latest = message_ts;
                            }
                        }
                        None => error!("not have _ts"),
                    }
                    self.messages
                        .insert(message_batch.get_name().to_string(), message);

                    self.messages.retain(|_, value| {
                        let _ts = value.get_i64("_ts");
                        match _ts {
                            Some(_ts) => {
                                if self.latest - _ts > self.duration {
                                    return false;
                                }
                            }
                            None => unreachable!(),
                        }
                        true
                    });
                    if self.messages.len() == self.count {
                        let message = Message::merge(&self.messages);
                        match self.tx.send(MessageBatch::from_message(message)) {
                            Ok(_) => {}
                            Err(e) => error!("send err:{}", e),
                        }
                        self.messages.clear();
                    }
                }
                Err(err) => {
                    error!("Error receiving message: {:?}", err);
                    return;
                    // Handle the error as needed
                }
            }
            // Check if all channels are closed
            if remaining_streams.is_empty() {
                error!("streams empty");
                break;
            }
        }
    }
}
