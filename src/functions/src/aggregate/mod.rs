use anyhow::{bail, Result};
use async_trait::async_trait;
use message::{Message, MessageBatch, MessageValue};
use serde::Deserialize;
use serde_json::Value;

use crate::Function;
mod avg;
mod max;
mod min;
mod sum;

pub trait Aggregater: Sync + Send {
    fn aggregate(&self, mb: &MessageBatch) -> MessageValue;
}

#[derive(Deserialize)]
pub struct Rule {
    r#type: String,
    in_field: String,
    out_field: String,
}

pub struct Aggregate {
    aggregaters: Vec<(Box<dyn Aggregater>, String)>,
}

impl Aggregate {
    pub fn new(conf: Value) -> Result<Self> {
        let rules: Vec<Rule> = serde_json::from_value(conf)?;

        let mut aggregaters = Vec::new();
        for rule in rules.iter() {
            match &rule.r#type.as_str() {
                &sum::TYPE => {
                    aggregaters.push((sum::new(rule.in_field.clone()), rule.out_field.clone()))
                }
                &avg::TYPE => {
                    aggregaters.push((avg::new(rule.in_field.clone()), rule.out_field.clone()))
                }
                &max::TYPE => {
                    aggregaters.push((max::new(rule.in_field.clone()), rule.out_field.clone()))
                }
                &min::TYPE => {
                    aggregaters.push((min::new(rule.in_field.clone()), rule.out_field.clone()))
                }
                _ => bail!("not support"),
            }
        }
        Ok(Aggregate { aggregaters })
    }
}

#[async_trait]
impl Function for Aggregate {
    async fn call(&self, message_batch: &mut MessageBatch) -> bool {
        let mut message = Message::default();
        for (aggregater, new_filed) in self.aggregaters.iter() {
            message.add(new_filed.clone(), aggregater.aggregate(&message_batch));
        }
        message_batch.clear();
        message_batch.push_message(message);
        true
    }
}
