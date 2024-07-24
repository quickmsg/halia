use anyhow::{bail, Result};
use message::MessageBatch;
use serde::Deserialize;
use tokio::sync::broadcast::{Receiver, Sender};

mod count;
mod time;

#[derive(Deserialize)]
struct Conf {
    typ: String,
    count: Option<u64>,
    // s
    interval: Option<u64>,
}

pub fn run(
    conf: serde_json::Value,
    rx: Receiver<MessageBatch>,
    tx: Sender<MessageBatch>,
    stop_signal_rx: Receiver<()>,
) -> Result<()> {
    let conf: Conf = serde_json::from_value(conf)?;
    match conf.typ.as_str() {
        count::TYPE => count::run(conf, rx, tx, stop_signal_rx),
        time::TYPE => time::run(conf, rx, tx, stop_signal_rx),
        _ => bail!("不支持"),
    }
}