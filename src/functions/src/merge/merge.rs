use std::{pin::Pin, sync::Arc};

use anyhow::Result;
use futures::Stream;
use message::{Message, MessageBatch};
use tokio::{
    select,
    sync::{
        broadcast::{self, Receiver, Sender},
        RwLock,
    },
};
use tokio_stream::{StreamExt, StreamMap};
use tracing::debug;

pub struct Merge {
    rxs: Vec<Receiver<MessageBatch>>,
    tx: Sender<MessageBatch>,
}

impl Merge {
    pub fn new(rxs: Vec<Receiver<MessageBatch>>, tx: Sender<MessageBatch>) -> Result<Merge> {
        Ok(Merge { rxs, tx })
    }

    pub async fn run(&mut self) {
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

pub async fn run(
    mut rxs: Vec<Receiver<MessageBatch>>,
    tx: Sender<MessageBatch>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) {
    let mut stream_map = StreamMap::new();
    let mut i = 0;
    while let Some(mut rx) = rxs.pop() {
        stream_map.insert(
            i,
            Box::pin(async_stream::stream! {
                  while let Ok(item) = rx.recv().await {
                      yield item;
                  }
            }) as Pin<Box<dyn Stream<Item = MessageBatch> + Send>>,
        );
        i += 1;
    }

    tokio::spawn(async move {
        loop {
            select! {
                Some((pos, mb)) = stream_map.next() => {
                    debug!("pos: {}, mb: {:?}", pos, mb);
                }

                _ = stop_signal_rx.recv() => {
                    debug!("stop signal received");
                    return;
                }
            }
        }
    });
}
