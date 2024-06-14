use anyhow::{bail, Result};
use gt::Gt;
use message::Message;
use serde::Deserialize;
use types::rule::Operate;

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

impl Operate for Node {
    fn operate(&self, message_batch: &mut message::MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        messages.retain(|message| {
            for filter in &self.filters {
                return filter.filter(message);
            }
            false
        });

        true
    }
}
