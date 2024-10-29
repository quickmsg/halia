use std::{collections::VecDeque, sync::Arc, time::Duration};

use anyhow::Result;
use common::timestamp_millis;
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
use types::rules::functions::window::TimeHopping;

pub fn run(
    conf: TimeHopping,
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
        let mut mbs = VecDeque::new();
        let start = Instant::now()
            .checked_add(Duration::from_micros(conf.interval))
            .unwrap();
        let mut interval = time::interval_at(start, Duration::from_micros(conf.interval));
        loop {
            select! {
                Some(rmb) = stream.next() => {
                    mbs.push_back((timestamp_millis(), rmb.take_mb()));
                }

                _ = interval.tick() => {
                    send_rule_message(conf.hopping, &txs, &mut mbs);
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
    mbs: &mut VecDeque<(u64, MessageBatch)>,
) {
    let mut send_mb = MessageBatch::default();
    loop {
        if let Some((ts, mb)) = mbs.front() {
            if timestamp_millis() - ts > hopping {
                send_mb.extend(mbs.pop_front().unwrap().1);
            } else {
                send_mb.extend(mb.clone());
            }
        } else {
            break;
        }
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
