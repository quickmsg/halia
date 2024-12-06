use anyhow::Result;
use async_trait::async_trait;
use message::{Message, MessageBatch, MessageValue};
use types::rules::functions::aggregation::{self, Conf};

use crate::nodes::Function;
mod avg;
mod collect;
mod count;
mod deduplicate;
mod max;
mod merge;
mod min;
mod sum;

pub trait Aggregater: Sync + Send {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue);
}

pub struct Aggregate {
    aggregaters: Vec<Box<dyn Aggregater>>,
}

pub fn new(conf: Conf) -> Result<Box<dyn Function>> {
    let mut aggregaters = Vec::with_capacity(conf.items.len());
    for item_conf in conf.items {
        let aggregater = match item_conf.typ {
            aggregation::Type::Sum => sum::new(item_conf),
            aggregation::Type::Avg => avg::new(item_conf),
            aggregation::Type::Max => max::new(item_conf),
            aggregation::Type::Min => min::new(item_conf),
            aggregation::Type::Count => count::new(item_conf),
            aggregation::Type::Collect => collect::new(item_conf),
            aggregation::Type::Merge => merge::new(item_conf),
            aggregation::Type::Deduplicate => deduplicate::new(item_conf),
        };

        aggregaters.push(aggregater);
    }
    Ok(Box::new(Aggregate { aggregaters }))
}

#[async_trait]
impl Function for Aggregate {
    async fn call(&mut self, message_batch: &mut MessageBatch) -> bool {
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

#[macro_export]
macro_rules! aggregate_return {
    ($self:expr, $result:expr) => {
        match &$self.target_field {
            Some(target_field) => (target_field.clone(), $result),
            None => ($self.field.clone(), $result),
        }
    };
}
