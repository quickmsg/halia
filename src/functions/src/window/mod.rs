use anyhow::{bail, Result};
use message::MessageBatch;
use tokio::sync::broadcast::{Receiver, Sender};
use types::rules::functions::WindowConf;

mod count;
mod time;

pub fn run(
    conf: WindowConf,
    rx: Receiver<MessageBatch>,
    tx: Sender<MessageBatch>,
    stop_signal_rx: Receiver<()>,
) -> Result<()> {
    match conf.typ.as_str() {
        count::TYPE => count::run(conf, rx, tx, stop_signal_rx),
        time::TYPE => time::run(conf, rx, tx, stop_signal_rx),
        _ => bail!("不支持"),
    }
}
