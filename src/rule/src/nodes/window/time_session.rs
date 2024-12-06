use std::{sync::Arc, time::Duration};

use anyhow::Result;
use message::{MessageBatch, RuleMessageBatch};
use tokio::{
    select,
    sync::{
        broadcast::Receiver,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    time::{self, Instant},
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tracing::error;
use types::rules::functions::window::TimeSession;

pub fn run(
    conf: TimeSession,
    rxs: Vec<UnboundedReceiver<RuleMessageBatch>>,
    txs: Vec<UnboundedSender<RuleMessageBatch>>,
    mut stop_signal_rx: Receiver<()>,
) -> Result<()> {
    let streams: Vec<_> = rxs
        .into_iter()
        .map(|rx| UnboundedReceiverStream::new(rx))
        .collect();

    let mut stream = futures::stream::select_all(streams);

    tokio::spawn(async move {
        let mut mbs = vec![];
        loop {
            let timeout = time::sleep(Duration::from_micros(conf.timeout));
            let mut empty = true;
            tokio::pin!(timeout);
            let max = time::sleep(Duration::from_micros(conf.max));
            tokio::pin!(max);
            select! {
                Some(rmb) = stream.next() => {
                    if empty {
                        empty = false;
                        max.as_mut().reset(Instant::now() + Duration::from_micros(conf.max));
                    }
                    mbs.push(rmb.take_mb());
                    timeout.as_mut().reset(Instant::now() + Duration::from_micros(conf.timeout));
                }

                _ = &mut timeout =>  {
                    send_rule_message(conf.timeout, &txs, &mut mbs);
                    empty = true;
                }

                _ = &mut max => {
                    send_rule_message(conf.max, &txs, &mut mbs);
                    empty = true;
                }

                _ = stop_signal_rx.recv() => {
                    return
                }
            }
        }
    });

    Ok(())
}

fn send_rule_message(
    hopping: u64,
    txs: &Vec<UnboundedSender<RuleMessageBatch>>,
    mbs: &mut Vec<MessageBatch>,
) {
    let mut send_mb = MessageBatch::default();
    for mb in mbs.iter() {
        send_mb.extend(mb.clone());
    }

    match txs.len() {
        0 => unreachable!(),
        1 => {
            let mb = RuleMessageBatch::Owned(send_mb);
            if let Err(e) = txs[0].send(mb) {
                error!("send rule message error: {}", e);
            }
        }
        _ => {
            let mb = RuleMessageBatch::Arc(Arc::new(send_mb));
            txs.iter().for_each(|tx| {
                if let Err(e) = tx.send(mb.clone()) {
                    error!("send rule message error: {}", e);
                }
            });
        }
    }
}
