use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::FilterConf;

use super::{get_target, Filter};

struct Gte {
    field: String,
    target_value: Option<MessageValue>,
    target_field: Option<String>,
}

pub const TYPE: &str = "gte";

pub fn new(conf: FilterConf) -> Result<Box<dyn Filter>> {
    let (target_value, target_field) = get_target(&conf)?;
    Ok(Box::new(Gte {
        field: conf.field,
        target_value,
        target_field,
    }))
}

impl Filter for Gte {
    fn filter(&self, msg: &Message) -> bool {
        let target_value = {
            if let Some(target_value) = &self.target_value {
                target_value
            } else if let Some(target_field) = &self.target_field {
                match msg.get(&target_field) {
                    Some(target_value) => target_value,
                    None => return false,
                }
            } else {
                unreachable!()
            }
        };

        match msg.get(&self.field) {
            Some(message_value) => match (message_value, target_value) {
                (MessageValue::Int64(_), MessageValue::Int64(_)) => todo!(),
                (MessageValue::Int64(_), MessageValue::Uint64(_)) => todo!(),
                (MessageValue::Int64(_), MessageValue::Float64(_)) => todo!(),
                (MessageValue::Int64(_), MessageValue::String(_)) => todo!(),
                (MessageValue::Int64(_), MessageValue::Bytes(_)) => todo!(),
                (MessageValue::Int64(_), MessageValue::Array(_)) => todo!(),
                (MessageValue::Int64(_), MessageValue::Object(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Null) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Boolean(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Int64(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Uint64(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Float64(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::String(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Bytes(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Array(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Object(_)) => todo!(),
                (MessageValue::Float64(_), MessageValue::Null) => todo!(),
                (MessageValue::Float64(_), MessageValue::Boolean(_)) => todo!(),
                (MessageValue::Float64(_), MessageValue::Int64(_)) => todo!(),
                (MessageValue::Float64(_), MessageValue::Uint64(_)) => todo!(),
                (MessageValue::Float64(_), MessageValue::Float64(_)) => todo!(),
                (MessageValue::Float64(_), MessageValue::String(_)) => todo!(),
                (MessageValue::Float64(_), MessageValue::Bytes(_)) => todo!(),
                (MessageValue::Float64(_), MessageValue::Array(_)) => todo!(),
                (MessageValue::Float64(_), MessageValue::Object(_)) => todo!(),
                (MessageValue::String(_), MessageValue::Null) => todo!(),
                (MessageValue::String(_), MessageValue::Boolean(_)) => todo!(),
                (MessageValue::String(_), MessageValue::Int64(_)) => todo!(),
                (MessageValue::String(_), MessageValue::Uint64(_)) => todo!(),
                (MessageValue::String(_), MessageValue::Float64(_)) => todo!(),
                (MessageValue::String(_), MessageValue::String(_)) => todo!(),
                (MessageValue::String(_), MessageValue::Bytes(_)) => todo!(),
                (MessageValue::String(_), MessageValue::Array(_)) => todo!(),
                (MessageValue::String(_), MessageValue::Object(_)) => todo!(),
                (MessageValue::Bytes(_), MessageValue::Bytes(_)) => todo!(),
                _ => false,
            },
            None => false,
        }
    }
}
