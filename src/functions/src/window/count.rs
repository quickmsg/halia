use anyhow::Result;
use message::MessageBatch;
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    select,
    sync::broadcast::{Receiver, Sender},
};

// pub struct Window {
//     conf: Conf,
//     message_batch: MessageBatch,
//     rx: Receiver<MessageBatch>,
//     tx: Sender<MessageBatch>,
//     stop_signal_rx: Receiver<()>,
// }

#[derive(Deserialize)]
struct Conf {
    pub count: u64,
}

pub fn run(
    conf: Value,
    mut rx: Receiver<MessageBatch>,
    tx: Sender<MessageBatch>,
    mut stop_signal_rx: Receiver<()>,
) -> Result<()> {
    let conf: Conf = serde_json::from_value(conf)?;
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
                            if cnt == conf.count {
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
