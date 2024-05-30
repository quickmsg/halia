use anyhow::{bail, Result};
use message::{Message, MessageBatch};
use serde::Deserialize;
use serde_json::Value;
use tracing::debug;
use types::rule::Operate;

use crate::aggregate::sum::SumFloat;

use self::{
    avg::{AvgFloat, AvgInt},
    max::{MaxFloat, MaxInt},
    min::{MinFloat, MinInt},
    sum::SumInt,
};

pub mod avg;
pub mod max;
pub mod min;
pub mod sum;

pub trait Aggregater: Sync + Send {
    fn aggregate(&self, mb: &MessageBatch) -> Value;
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
                &"sum" => match &rule.option.as_str() {
                    &"int" => aggregaters.push(SumInt::new(rule.field.clone())),
                    &"float" => aggregaters.push(SumFloat::new(rule.field.clone())),
                    _ => bail!("not support"),
                },
                &"avg" => match &rule.option.as_str() {
                    &"int" => aggregaters.push(AvgInt::new(rule.field.clone())),
                    &"float" => aggregaters.push(AvgFloat::new(rule.field.clone())),
                    _ => bail!("not support"),
                },
                &"max" => match &rule.option.as_str() {
                    &"int" => aggregaters.push(MaxInt::new(rule.field.clone())),
                    &"float" => aggregaters.push(MaxFloat::new(rule.field.clone())),
                    _ => bail!("not support"),
                },
                &"min" => match &rule.option.as_str() {
                    &"int" => aggregaters.push(MinInt::new(rule.field.clone())),
                    &"float" => aggregaters.push(MinFloat::new(rule.field.clone())),
                    _ => bail!("not support"),
                },
                _ => bail!("not support"),
            }
        }
        Ok(Node { aggregaters, rules })
    }
}

impl Operate for Node {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        let mut message = Message::new();
        for (index, rule) in self.rules.iter().enumerate() {
            let value = self.aggregaters[index].aggregate(&message_batch);
            debug!("value:{:?}", value);
            match &rule.name {
                Some(name) => message.add(name, value),
                None => message.add(&rule.field, value),
            }
        }
        debug!("message is:{:?}", message);
        message_batch.clear();
        message_batch.push(message);
        true
    }
}
