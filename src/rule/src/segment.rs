use std::sync::Arc;

use common::log::LoggerItem;
use futures::StreamExt;
use message::RuleMessageBatch;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::nodes::Function;

pub(crate) fn start_segment(
    rxs: Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>,
    mut functions: Vec<Box<dyn Function>>,
    txs: Vec<mpsc::UnboundedSender<RuleMessageBatch>>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) {
    let streams: Vec<_> = rxs
        .into_iter()
        .map(|rx| UnboundedReceiverStream::new(rx))
        .collect();

    let mut stream = futures::stream::select_all(streams);

    tokio::spawn(async move {
        loop {
            select! {
                Some(mb) = stream.next() => {
                    handle_segment_mb(mb, &mut functions, &txs).await;
                }
                _ = stop_signal_rx.recv() => {
                    return
                }
            }
        }
    });
}

async fn handle_segment_mb(
    mb: RuleMessageBatch,
    functions: &mut Vec<Box<dyn Function>>,
    txs: &Vec<mpsc::UnboundedSender<RuleMessageBatch>>,
) {
    let mut mb = mb.take_mb();
    for function in functions.iter_mut() {
        if !function.call(&mut mb).await {
            return;
        }
    }

    match txs.len() {
        0 => {}
        1 => {
            let _ = txs[0].send(RuleMessageBatch::Owned(mb));
        }
        _ => {
            let mb = Arc::new(mb);
            for tx in txs.iter() {
                let _ = tx.send(RuleMessageBatch::Arc(mb.clone()));
            }
        }
    }
}

pub struct BlackHole {
    rxs: Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>,
    logger: LoggerItem,
}

impl BlackHole {
    pub fn new(logger: LoggerItem) -> Self {
        BlackHole {
            rxs: vec![],
            logger,
        }
    }

    pub fn get_txs(&mut self, cnt: usize) -> Vec<mpsc::UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];
        for _ in 0..cnt {
            let (tx, rx) = mpsc::unbounded_channel();
            self.rxs.push(rx);
            txs.push(tx);
        }
        txs
    }

    pub fn run(mut self, mut stop_signal_rx: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            match self.rxs.len() {
                0 => unreachable!(),
                1 => {
                    let mut rx = self.rxs.pop().unwrap();
                    tokio::spawn(async move {
                        loop {
                            select! {
                                Some(rmb) = rx.recv() => {
                                    if self.logger.is_enable() {
                                        self.logger.log(format!("black hole received msg: {:?}", rmb.take_mb()));
                                    }
                                }
                                _ = stop_signal_rx.recv() => {
                                    return
                                }
                            }
                        }
                    });
                }
                _ => {
                    let streams: Vec<_> = self
                        .rxs
                        .into_iter()
                        .map(|rx| UnboundedReceiverStream::new(rx))
                        .collect();
                    let mut stream = futures::stream::select_all(streams);
                    tokio::spawn(async move {
                        loop {
                            select! {
                                Some(rmb) = stream.next() => {
                                    if self.logger.is_enable() {
                                        self.logger.log(format!("black hole received msg: {:?}", rmb.take_mb()));
                                    }
                                }
                                _ = stop_signal_rx.recv() => {
                                    return
                                }
                            }
                        }
                    });
                }
            }
        });
    }
}
