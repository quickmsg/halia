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
use types::rules::functions::window::TimeThmbling;

pub fn run(
    conf: TimeThmbling,
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
        let mut mb = MessageBatch::default();
        let start = Instant::now()
            .checked_add(Duration::from_micros(conf.interval))
            .unwrap();
        let mut interval = time::interval_at(start, Duration::from_micros(conf.interval));
        loop {
            select! {
                Some(rmb) = stream.next() => {
                    mb.extend(rmb.take_mb());
                }

                _ = interval.tick() => {
                    send_rule_message(&txs, mb);
                    mb = MessageBatch::default();
                }

                _ = stop_signal_rx.recv() => {
                    return
                }
            }
        }
    });

    Ok(())
}

fn send_rule_message(txs: &Vec<UnboundedSender<RuleMessageBatch>>, mb: MessageBatch) {
    match txs.len() {
        0 => unreachable!(),
        1 => {
            let mb = RuleMessageBatch::Owned(mb);
            if let Err(e) = txs[0].send(mb) {
                error!("send rule message error: {}", e);
            }
        }
        _ => {
            let mb = RuleMessageBatch::Arc(Arc::new(mb));
            txs.iter().for_each(|tx| {
                if let Err(e) = tx.send(mb.clone()) {
                    error!("send rule message error: {}", e);
                }
            });
        }
    }
}
