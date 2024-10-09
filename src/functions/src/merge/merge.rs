use std::pin::Pin;

use futures::Stream;
use message::{Message, MessageBatch};
use tokio::{select, sync::broadcast};
use tokio_stream::{StreamExt, StreamMap};
use tracing::debug;

pub fn run(
    mut rxs: Vec<broadcast::Receiver<MessageBatch>>,
    tx: broadcast::Sender<MessageBatch>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) {
    let mut msgs: Vec<Option<Message>> = Vec::with_capacity(rxs.len());

    let mut stream_map = StreamMap::new();
    let mut i: u8 = 0;
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
                    handle_mb(&mut msgs, &tx, pos, mb);
                }

                _ = stop_signal_rx.recv() => {
                    debug!("stop signal received");
                    return;
                }
            }
        }
    });
}

fn handle_mb(
    msgs: &mut Vec<Option<Message>>,
    tx: &broadcast::Sender<MessageBatch>,
    pos: u8,
    mut mb: MessageBatch,
) {
    let message = mb.take_one_message();
    msgs[pos as usize] = message;

    if msgs.iter().all(|msg| msg.is_some()) {
        let mut mb = MessageBatch::default();
        for msg in msgs.iter_mut() {
            mb.push_message(msg.take().unwrap());
        }
        tx.send(mb).unwrap();
    }
}
