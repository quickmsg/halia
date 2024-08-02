use anyhow::{bail, Result};
use message::Message;
use types::rules::functions::FilterConf;

use crate::Function;

// mod ct;
mod eq;
mod gt;
mod gte;
mod lt;
mod lte;
mod neq;
// mod reg;

pub trait Filter: Sync + Send {
    fn filter(&self, message: &Message) -> bool;
}

pub struct Node {
    filters: Vec<Box<dyn Filter>>,
}

pub fn new(confs: Vec<FilterConf>) -> Result<Box<dyn Function>> {
    let mut filters: Vec<Box<dyn Filter>> = Vec::with_capacity(confs.len());
    for conf in confs {
        let filter = match conf.typ {
            types::rules::functions::FilterType::Eq => eq::new(conf)?,
            types::rules::functions::FilterType::Gt => gt::new(conf)?,
            types::rules::functions::FilterType::Gte => gte::new(conf)?,
            types::rules::functions::FilterType::Lt => lt::new(conf)?,
            types::rules::functions::FilterType::Lte => lte::new(conf)?,
            types::rules::functions::FilterType::Neq => neq::new(conf)?,
            _ => bail!("not support"),
        };
        filters.push(filter);
    }
    Ok(Box::new(Node { filters }))
}

impl Function for Node {
    fn call(&self, message_batch: &mut message::MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
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
