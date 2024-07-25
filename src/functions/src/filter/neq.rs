use anyhow::{bail, Result};
use message::{Message, MessageValue};
use serde::{Deserialize, Serialize};
use types::rules::functions::FilterConf;

use super::{get_target, Filter};

struct Neq {
    field: String,
    target_value: Option<MessageValue>,
    target_field: Option<String>,
}

pub const TYPE: &str = "neq";

pub fn new(conf: FilterConf) -> Result<Box<dyn Filter>> {
    let (target_value, target_field) = get_target(&conf)?;
    Ok(Box::new(Neq {
        field: conf.field,
        target_value,
        target_field,
    }))
}

impl Filter for Neq {
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
                (MessageValue::Null, MessageValue::Null) => false,
                (MessageValue::Boolean(mv), MessageValue::Boolean(tv)) => mv != tv,
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
                (MessageValue::Bytes(_), MessageValue::Null) => todo!(),
                (MessageValue::Bytes(_), MessageValue::Boolean(_)) => todo!(),
                (MessageValue::Bytes(_), MessageValue::Int64(_)) => todo!(),
                (MessageValue::Bytes(_), MessageValue::Uint64(_)) => todo!(),
                (MessageValue::Bytes(_), MessageValue::Float64(_)) => todo!(),
                (MessageValue::Bytes(_), MessageValue::String(_)) => todo!(),
                (MessageValue::Bytes(_), MessageValue::Bytes(_)) => todo!(),
                (MessageValue::Bytes(_), MessageValue::Array(_)) => todo!(),
                (MessageValue::Bytes(_), MessageValue::Object(_)) => todo!(),
                (MessageValue::Array(_), MessageValue::Null) => todo!(),
                (MessageValue::Array(_), MessageValue::Boolean(_)) => todo!(),
                (MessageValue::Array(_), MessageValue::Int64(_)) => todo!(),
                (MessageValue::Array(_), MessageValue::Uint64(_)) => todo!(),
                (MessageValue::Array(_), MessageValue::Float64(_)) => todo!(),
                (MessageValue::Array(_), MessageValue::String(_)) => todo!(),
                (MessageValue::Array(_), MessageValue::Bytes(_)) => todo!(),
                (MessageValue::Array(_), MessageValue::Array(_)) => todo!(),
                (MessageValue::Array(_), MessageValue::Object(_)) => todo!(),
                (MessageValue::Object(_), MessageValue::Null) => todo!(),
                (MessageValue::Object(_), MessageValue::Boolean(_)) => todo!(),
                (MessageValue::Object(_), MessageValue::Int64(_)) => todo!(),
                (MessageValue::Object(_), MessageValue::Uint64(_)) => todo!(),
                (MessageValue::Object(_), MessageValue::Float64(_)) => todo!(),
                (MessageValue::Object(_), MessageValue::String(_)) => todo!(),
                (MessageValue::Object(_), MessageValue::Bytes(_)) => todo!(),
                (MessageValue::Object(_), MessageValue::Array(_)) => todo!(),
                (MessageValue::Object(_), MessageValue::Object(_)) => todo!(),
                _ => false,
            },
            None => false,
        }
    }
}
