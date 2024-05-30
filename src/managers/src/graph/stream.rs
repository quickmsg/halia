use anyhow::{bail, Result};
use message::MessageBatch;
use operators::{
    aggregate::Node as AggregateNode,
    computes::ComputeNode,
    filter::Node as FilterNode, // merge::merge::Merge,
};
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{debug, error};
use types::rule::{CreateGraphNode, Operate};

pub struct Stream {
    pub rx: Receiver<MessageBatch>,
    pub nodes: Vec<Box<dyn Operate>>,
    pub tx: Sender<MessageBatch>,
}

impl Stream {
    pub fn new(
        rx: Receiver<MessageBatch>,
        create_graph_nodes: &Vec<&CreateGraphNode>,
        tx: Sender<MessageBatch>,
    ) -> Result<Self> {
        let mut nodes = Vec::new();
        for create_graph_node in create_graph_nodes.iter() {
            let node = get_operator_node(&create_graph_node)?;
            nodes.push(node);
        }
        Ok(Stream { rx, nodes, tx })
    }

    pub async fn run(&mut self) {
        debug!("stream run");
        loop {
            match self.rx.recv().await {
                Ok(mut message_batch) => {
                    for node in self.nodes.iter() {
                        node.operate(&mut message_batch);
                    }
                    match self.tx.send(message_batch) {
                        Err(e) => {
                            error!("stream send err:{}, ids", e);
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    debug!("stream recv error: {:?}", e);
                }
            }
        }
    }
}

fn get_operator_node(cgn: &CreateGraphNode) -> Result<Box<dyn Operate>> {
    debug!("{:?}", cgn);
    match cgn.r#type.as_str() {
        "field" => match cgn.name.as_str() {
            // "watermark" => {
            //     let watermark = Watermark::new(cgn.conf.clone())?;
            //     Ok(Box::new(watermark))
            // }
            // "name" => {
            //     let name = Name::new(cgn.conf.clone())?;
            //     Ok(Box::new(name))
            // }
            _ => bail!("not support"),
        },
        "filter" => {
            let filter = FilterNode::new(cgn.conf.clone())?;
            Ok(Box::new(filter))
        }
        "compute" => {
            ComputeNode::new(cgn.conf.clone())
        }
        "aggregate" => {
            let aggregate = AggregateNode::new(cgn.conf.clone())?;
            Ok(Box::new(aggregate))
        }
        _ => bail!("not support"),
    }
}
