use anyhow::Result;
use message::MessageBatch;
use tokio::{select, sync::broadcast};
use tracing::{debug, error};
use types::rule::CreateRuleNode;

pub struct Stream {
    nodes: Vec<CreateRuleNode>,
}

impl Stream {
    pub fn new(nodes: Vec<CreateRuleNode>) -> Result<Self> {
        // TODO validate
        Ok(Stream { nodes })
    }

    pub async fn start(
        &self,
        mut rx: broadcast::Receiver<MessageBatch>,
        tx: broadcast::Sender<MessageBatch>,
        mut stop_signal: broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut functions = Vec::new();
        for create_graph_node in &self.nodes {
            let node = functions::new(&create_graph_node)?;
            functions.push(node);
        }

        tokio::spawn(async move {
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
        });

        Ok(())
    }
}
