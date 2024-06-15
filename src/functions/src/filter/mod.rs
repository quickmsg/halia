use anyhow::{bail, Result};
use gt::Gt;
use message::Message;
use serde::Deserialize;
use tracing::debug;
use types::rule::Operate;

use crate::Function;

pub mod gt;

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
                "gt" => {
                    let gt = Gt::new(conf.conf)?;
                    filters.push(Box::new(gt));
                }
                _ => bail!("not support"),
            }
        }
        Ok(Node { filters })
    }
}

impl Function for Node {
    fn call(&self, message_batch: &mut message::MessageBatch) {
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
        debug!("{:?}", messages);
    }
}
