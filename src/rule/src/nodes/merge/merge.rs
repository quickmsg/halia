use std::{pin::Pin, sync::Arc};

use futures::Stream;
use message::{Message, MessageBatch, RuleMessageBatch};
use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tokio_stream::{StreamExt, StreamMap};

pub fn run(
    mut rxs: Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>,
    txs: Vec<mpsc::UnboundedSender<RuleMessageBatch>>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) {
    let mut msgs: Vec<Option<Message>> = vec![None; rxs.len()];

    let mut stream_map = StreamMap::new();
    let mut i: u8 = 0;
    while let Some(mut rx) = rxs.pop() {
        stream_map.insert(
            i,
            Box::pin(async_stream::stream! {
                  while let Some(item) = rx.recv().await {
                      yield item;
                  }
            }) as Pin<Box<dyn Stream<Item = RuleMessageBatch> + Send>>,
        );
        i += 1;
    }

    tokio::spawn(async move {
        loop {
            select! {
                Some((pos, mb)) = stream_map.next() => {
                    handle_mb(&mut msgs, &txs, pos, mb);
                }

                _ = stop_signal_rx.recv() => {
                    return;
                }
            }
        }
    });
}

fn handle_mb(
    msgs: &mut Vec<Option<Message>>,
    txs: &Vec<mpsc::UnboundedSender<RuleMessageBatch>>,
    pos: u8,
    mb: RuleMessageBatch,
) {
    let mut mb = mb.take_mb();
    let message = mb.take_one_message();
    msgs[pos as usize] = message;

    if msgs.iter().all(|msg| msg.is_some()) {
        let mut merge_mb = MessageBatch::default();
        let mut merge_msg = Message::default();
        for msg in msgs.iter_mut() {
            merge_msg.merge(msg.take().unwrap());
        }
        merge_mb.push_message(merge_msg);

        match txs.len() {
            1 => {
                txs[0].send(RuleMessageBatch::Owned(merge_mb)).unwrap();
            }
            _ => {
                let merge_mb = Arc::new(merge_mb);
                for tx in txs.iter() {
                    tx.send(RuleMessageBatch::Arc(merge_mb.clone())).unwrap();
                }
            }
        }
    }
}