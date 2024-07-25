use std::time::Duration;

use anyhow::{bail, Result};
use message::MessageBatch;
use tokio::{
    select,
    sync::broadcast::{Receiver, Sender},
    time::{self, Instant},
};
use types::rules::functions::WindowConf;

pub const TYPE: &str = "time";

pub fn run(
    conf: WindowConf,
    mut rx: Receiver<MessageBatch>,
    tx: Sender<MessageBatch>,
    mut stop_signal_rx: Receiver<()>,
) -> Result<()> {
    let interval = match conf.interval {
        Some(interval) => interval,
        None => bail!("未填写interval值"),
    };
    tokio::spawn(async move {
        let mut mb = MessageBatch::default();
        let start = Instant::now()
            .checked_add(Duration::from_secs(interval))
            .unwrap();
        let mut interval = time::interval_at(start, Duration::from_secs(interval));
        loop {
            select! {
                in_mb = rx.recv() => {
                    match in_mb {
                        Ok(in_mb) => {
                            mb.extend(in_mb);
                        }
                        Err(_) => return,
                    }
                }

                _ = interval.tick() => {
                    tx.send(mb).unwrap();
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
