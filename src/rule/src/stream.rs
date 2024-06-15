use message::MessageBatch;
use tokio::{select, sync::broadcast};
use tracing::{debug, error};
use types::rule::CreateRuleNode;

pub(crate) async fn start_stream(
    nodes: Vec<&CreateRuleNode>,
    mut rx: broadcast::Receiver<MessageBatch>,
    tx: broadcast::Sender<MessageBatch>,
    mut stop_signal: broadcast::Receiver<()>,
) {
    let mut functions = Vec::new();
    for create_graph_node in nodes {
        let node = functions::new(&create_graph_node).unwrap();
        functions.push(node);
    }

    loop {
        select! {
            biased;

            _ = stop_signal.recv() => {
                debug!("stream stop");
                return
            }

            message_batch = rx.recv() => {
                match message_batch {
                    Ok(mut message_batch) => {
                        for function in &functions {
                            function.call(&mut message_batch);
                            if message_batch.len() == 0 {
                                break;
                            }
                        }

                        if message_batch.len() != 0 {
                            if let Err(e) = tx.send(message_batch) {
                                error!("stream send err:{}, ids", e);
                            }
                        }
                    },
                    Err(e) => {
                        error!("stream recv err:{}", e);
                        break;
                    }
                }
            }
        }
    }
}