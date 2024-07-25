use anyhow::{bail, Result};
use message::MessageBatch;
use tokio::{
    select,
    sync::broadcast::{Receiver, Sender},
};
use types::rules::functions::WindowConf;

pub const TYPE: &str = "count";

pub fn run(
    conf: WindowConf,
    mut rx: Receiver<MessageBatch>,
    tx: Sender<MessageBatch>,
    mut stop_signal_rx: Receiver<()>,
) -> Result<()> {
    let conf_cnt = match conf.count {
        Some(count) => count,
        None => bail!("未填写count"),
    };
    tokio::spawn(async move {
        let mut mb = MessageBatch::default();
        let mut cnt: u64 = 0;
        loop {
            select! {
                in_mb = rx.recv() => {
                    match in_mb {
                        Ok(in_mb) => {
                            mb.extend(in_mb);
                            cnt += 1;
                            if cnt == conf_cnt {
                                tx.send(mb).unwrap();
                            }
                            mb = MessageBatch::default();
                        }
                        Err(_) => return,
                    }
                }

                _ = stop_signal_rx.recv() => {
                    return
                }
            }
        }
    });

    Ok(())
}
