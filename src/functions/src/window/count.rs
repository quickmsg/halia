use std::sync::Arc;

use anyhow::Result;
use message::{MessageBatch, RuleMessageBatch};
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use types::rules::functions::window::Count;

pub fn run(
    conf: Count,
    mut rxs: Vec<UnboundedReceiver<RuleMessageBatch>>,
    txs: Vec<UnboundedSender<RuleMessageBatch>>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) -> Result<()> {
    tokio::spawn(async move {
        let mut mbs = vec![];
        let mut cnt: u64 = 0;

        if rxs.len() == 0 {
            loop {
                select! {
                    Some(rmb) = rxs[0].recv() => {
                        mbs.push(rmb.take_mb());
                        cnt += 1;
                        if cnt == conf.count {
                            cnt = 0;
                            send_rule_message(&txs, &mut mbs);
                        }
                    }

                    _ = stop_signal_rx.recv() => {
                        return
                    }
                }
            }
        } else {
            let streams: Vec<_> = rxs
                .into_iter()
                .map(|rx| UnboundedReceiverStream::new(rx))
                .collect();

            let mut stream = futures::stream::select_all(streams);
            loop {
                select! {
                    Some(rmb) = stream.next() => {
                        mbs.push(rmb.take_mb());
                        cnt += 1;
                        if cnt == conf.count {
                            cnt = 0;
                            send_rule_message(&txs, &mut mbs);
                        }
                    }

                    _ = stop_signal_rx.recv() => {
                        return
                    }
                }
            }
        }
    });

    Ok(())
}

fn send_rule_message(txs: &Vec<UnboundedSender<RuleMessageBatch>>, mbs: &mut Vec<MessageBatch>) {
    let mut send_mb = MessageBatch::default();
    for mb in mbs.drain(..) {
        send_mb.extend(mb);
    }

    match txs.len() {
        0 => unreachable!(),
        1 => {
            let rmb = RuleMessageBatch::Owned(send_mb);
            txs[0].send(rmb).unwrap();
        }
        _ => {
            let rmb = RuleMessageBatch::Arc(Arc::new(send_mb));
            txs.iter().for_each(|tx| tx.send(rmb.clone()).unwrap());
        }
    }
}
