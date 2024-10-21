use anyhow::Result;
use async_trait::async_trait;
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
mod reg;

pub(crate) trait Filter: Sync + Send {
    fn filter(&self, msg: &Message) -> bool;
}

pub struct Node {
    filters: Vec<Box<dyn Filter>>,
}

pub fn new(conf: FilterConf) -> Result<Box<dyn Function>> {
    let mut filters: Vec<Box<dyn Filter>> = Vec::with_capacity(conf.filters.len());
    for conf_item in conf.filters {
        let filter = match conf_item.typ {
            types::rules::functions::FilterType::Eq => eq::new(conf_item.field, conf_item.value)?,
            types::rules::functions::FilterType::Gt => gt::new(conf_item.field, conf_item.value)?,
            types::rules::functions::FilterType::Gte => gte::new(conf_item.field, conf_item.value)?,
            types::rules::functions::FilterType::Lt => lt::new(conf_item.field, conf_item.value)?,
            types::rules::functions::FilterType::Lte => lte::new(conf_item.field, conf_item.value)?,
            types::rules::functions::FilterType::Neq => neq::new(conf_item.field, conf_item.value)?,
            types::rules::functions::FilterType::Ct => ct::new(conf_item.field, conf_item.value)?,
            types::rules::functions::FilterType::Reg => reg::new(conf_item.field, conf_item.value)?,
        };
        filters.push(filter);
    }
    Ok(Box::new(Node { filters }))
}

#[async_trait]
impl Function for Node {
    async fn call(&self, message_batch: &mut message::MessageBatch) -> bool {
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
