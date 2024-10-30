use anyhow::Result;
use message::{MessageBatch, RuleMessageBatch};
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use types::rules::functions::window::Count;

pub fn run(
    conf: Count,
    rxs: Vec<UnboundedReceiver<RuleMessageBatch>>,
    txs: Vec<UnboundedSender<RuleMessageBatch>>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) -> Result<()> {
    tokio::spawn(async move {
        let mut mb = MessageBatch::default();
        let mut cnt: u64 = 0;
        let streams: Vec<_> = rxs
            .into_iter()
            .map(|rx| UnboundedReceiverStream::new(rx))
            .collect();

        let mut stream = futures::stream::select_all(streams);
        loop {
            select! {
                in_mb = stream.next() => {
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
