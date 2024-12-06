use anyhow::{bail, Result};
use message::RuleMessageBatch;
use tokio::sync::{
    broadcast,
    mpsc::{UnboundedReceiver, UnboundedSender},
};
use types::rules::functions::window::Conf;

mod count;
mod time_hopping;
mod time_session;
mod time_sliding;
mod time_thmbling;

pub fn run(
    conf: Conf,
    rxs: Vec<UnboundedReceiver<RuleMessageBatch>>,
    txs: Vec<UnboundedSender<RuleMessageBatch>>,
    stop_signal_rx: broadcast::Receiver<()>,
) -> Result<()> {
    match conf.typ {
        types::rules::functions::window::Type::TimeThmbling => match conf.time_thmbling {
            Some(time_thmbling) => time_thmbling::run(time_thmbling, rxs, txs, stop_signal_rx),
            None => bail!("time_thmbling is required"),
        },
        types::rules::functions::window::Type::TimeHopping => match conf.time_hopping {
            Some(time_hopping) => time_hopping::run(time_hopping, rxs, txs, stop_signal_rx),
            None => bail!("time_hopping is required"),
        },
        types::rules::functions::window::Type::TimeSession => match conf.time_session {
            Some(time_session) => time_session::run(time_session, rxs, txs, stop_signal_rx),
            None => bail!("time_session is required"),
        },
        types::rules::functions::window::Type::Count => match conf.count {
            Some(count) => count::run(count, rxs, txs, stop_signal_rx),
            None => bail!("count is required"),
        },
    }
}
