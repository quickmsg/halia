use anyhow::Result;
use message::Message;
use types::rules::functions::FilterConf;

use crate::Function;

mod ct;
mod eq;
mod gt;
mod gte;
mod lt;
mod lte;
mod neq;
// mod reg;

pub(crate) trait Filter: Sync + Send {
    fn filter(&self, msg: &Message) -> bool;
}

pub struct Node {
    filters: Vec<Box<dyn Filter>>,
}

pub fn new(conf: FilterConf) -> Result<Box<dyn Function>> {
    let mut filters: Vec<Box<dyn Filter>> = Vec::with_capacity(conf.filters.len());
    for conf in conf.filters {
        let filter = match conf.typ {
            types::rules::functions::FilterType::Eq => eq::new(conf.field, conf.value)?,
            types::rules::functions::FilterType::Gt => gt::new(conf.field, conf.value)?,
            types::rules::functions::FilterType::Gte => gte::new(conf.field, conf.value)?,
            types::rules::functions::FilterType::Lt => lt::new(conf.field, conf.value)?,
            types::rules::functions::FilterType::Lte => lte::new(conf.field, conf.value)?,
            types::rules::functions::FilterType::Neq => neq::new(conf.field, conf.value)?,
            types::rules::functions::FilterType::Ct => ct::new(conf.field, conf.value)?,
            types::rules::functions::FilterType::Reg => todo!(),
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
