use anyhow::Result;
use async_trait::async_trait;
use message::{Message, MessageBatch, MessageValue};
use serde::Deserialize;
use types::rules::functions::aggregate::{self, Conf};

use crate::Function;
mod avg;
mod count;
mod max;
mod min;
mod sum;

pub trait Aggregater: Sync + Send {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue);
}

#[derive(Deserialize)]
pub struct Rule {
    r#type: String,
    in_field: String,
    out_field: String,
}

pub struct Aggregate {
    aggregaters: Vec<Box<dyn Aggregater>>,
}

impl Aggregate {
    pub fn new(conf: Conf) -> Result<Self> {
        let mut aggregaters = Vec::with_capacity(conf.items.len());
        for item_conf in conf.items {
            let aggregater = match item_conf.typ {
                aggregate::Type::Sum => sum::new(item_conf),
                aggregate::Type::Avg => avg::new(item_conf),
                aggregate::Type::Max => max::new(item_conf),
                aggregate::Type::Min => min::new(item_conf),
                aggregate::Type::Count => count::new(item_conf),
            };

            aggregaters.push(aggregater);
        }
        Ok(Aggregate { aggregaters })
    }
}

#[async_trait]
impl Function for Aggregate {
    async fn call(&self, message_batch: &mut MessageBatch) -> bool {
        let mut message = Message::default();
        for aggregater in self.aggregaters.iter() {
            let (new_field, value) = aggregater.aggregate(&message_batch);
            message.add(new_field, value);
        }
        message_batch.clear();
        message_batch.push_message(message);
        true
    }
}