use anyhow::{bail, Result};
use avg::Avg;
use max::Max;
use message::{Message, MessageBatch, MessageValue};
use min::Min;
use serde::Deserialize;
use serde_json::Value;
use sum::Sum;
use tracing::debug;

pub mod avg;
pub mod max;
pub mod min;
pub mod sum;

pub trait Aggregater: Sync + Send {
    fn aggregate(&self, mb: &MessageBatch) -> MessageValue;
}

#[derive(Deserialize)]
pub struct Rule {
    r#type: String,
    option: String,
    field: String,
    name: Option<String>,
}

pub struct Node {
    aggregaters: Vec<Box<dyn Aggregater>>,
    rules: Vec<Rule>,
}

impl Node {
    pub fn new(conf: Value) -> Result<Self> {
        let mut rules: Vec<Rule> = serde_json::from_value(conf)?;

        let mut aggregaters = Vec::new();
        for rule in rules.iter_mut() {
            match &rule.r#type.as_str() {
                &"sum" => aggregaters.push(Sum::new(rule.field.clone())),
                &"avg" => aggregaters.push(Avg::new(rule.field.clone())),
                &"max" => aggregaters.push(Max::new(rule.field.clone())),
                &"min" => aggregaters.push(Min::new(rule.field.clone())),
                _ => bail!("not support"),
            }
        }
        Ok(Node { aggregaters, rules })
    }
}

impl Operate for Node {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        let mut message = Message::default();
        for (index, rule) in self.rules.iter().enumerate() {
            let value = self.aggregaters[index].aggregate(&message_batch);
            debug!("value:{:?}", value);
            match &rule.name {
                Some(name) => message.add(name.clone(), value),
                None => message.add(rule.field.clone(), value),
            }
        }
        debug!("message is:{:?}", message);
        // message_batch.clear();
        // message_batch.push(message);
        todo!();
        true
    }
}
