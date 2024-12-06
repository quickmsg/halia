use anyhow::Result;
use async_trait::async_trait;
use common::log::LoggerItem;
use message::Message;
use types::rules::functions::filter::Conf;

use super::Function;

mod ct;
mod eq;
mod gt;
mod gte;
mod lt;
mod lte;
mod neq;
mod reg;

#[async_trait]
pub(crate) trait Filter: Sync + Send {
    async fn filter(&self, msg: &Message) -> bool;
}

pub struct Node {
    filters: Vec<Box<dyn Filter>>,
}

pub fn new(conf: Conf, logger: LoggerItem) -> Result<Box<dyn Function>> {
    let mut filters: Vec<Box<dyn Filter>> = Vec::with_capacity(conf.items.len());
    for item_conf in conf.items {
        let filter = match item_conf.typ {
            types::rules::functions::filter::Type::Ct => ct::new(item_conf, logger.clone())?,
            types::rules::functions::filter::Type::Eq => eq::new(item_conf)?,
            types::rules::functions::filter::Type::Gt => gt::new(item_conf)?,
            types::rules::functions::filter::Type::Gte => gte::new(item_conf)?,
            types::rules::functions::filter::Type::Lt => lt::new(item_conf)?,
            types::rules::functions::filter::Type::Lte => lte::new(item_conf)?,
            types::rules::functions::filter::Type::Neq => neq::new(item_conf)?,
            types::rules::functions::filter::Type::Reg => reg::new(item_conf)?,
            types::rules::functions::filter::Type::IsArray => todo!(),
        };
        filters.push(filter);
    }
    Ok(Box::new(Node { filters }))
}

#[async_trait]
impl Function for Node {
    async fn call(&mut self, message_batch: &mut message::MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        for message in messages.iter_mut() {
            let mut passed = false;
            for filter in &self.filters {
                if filter.filter(message).await {
                    passed = true;
                    break;
                }
            }
            if !passed {
                message.insert_metadata("keep".to_owned(), message::MessageValue::Boolean(false));
            }
        }

        messages.retain_mut(|message| {
            if let Some(value) = message.get_metadata("keep") {
                match value {
                    message::MessageValue::Boolean(b) => return *b,
                    _ => todo!(),
                }
            }
            true
        });

        message_batch.len() != 0
    }
}
