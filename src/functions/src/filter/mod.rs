use anyhow::{bail, Result};
use message::Message;
use serde::Deserialize;
use tracing::debug;

use crate::Function;

mod ct;
mod eq;
mod gt;
mod gte;
mod neq;
mod reg;

pub trait Filter: Sync + Send {
    fn filter(&self, message: &Message) -> bool;
}

pub struct Node {
    filters: Vec<Box<dyn Filter>>,
}

#[derive(Deserialize)]
struct Conf {
    r#type: String,
    conf: serde_json::Value,
}

impl Node {
    pub fn new(conf: serde_json::Value) -> Result<Self> {
        let confs: Vec<Conf> = serde_json::from_value(conf)?;
        let mut filters: Vec<Box<dyn Filter>> = Vec::with_capacity(confs.len());
        for conf in confs {
            match conf.r#type.as_str() {
                gt::TYPE => filters.push(gt::new(conf.conf)?),
                gte::TYPE => filters.push(gte::new(conf.conf)?),
                // "lt" => {
                //     let lt = Lt::new(conf.conf)?;
                //     filters.push(Box::new(lt));
                // }
                // "lte" => {
                //     let lte = Lte::new(conf.conf)?;
                //     filters.push(Box::new(lte));
                // }
                eq::TYPE => filters.push(eq::new(conf.conf)?),
                neq::TYPE => filters.push(neq::new(conf.conf)?),
                ct::TYPE => filters.push(ct::new(conf.conf)?),
                reg::TYPE => filters.push(reg::new(conf.conf)?),
                _ => bail!("not support"),
            }
        }
        Ok(Node { filters })
    }
}

impl Function for Node {
    fn call(&self, message_batch: &mut message::MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        debug!("{:?}", messages);
        messages.retain(|message| {
            for filter in &self.filters {
                if filter.filter(message) {
                    return true;
                }
            }
            false
        });

        message_batch.len() != 0
    }
}
