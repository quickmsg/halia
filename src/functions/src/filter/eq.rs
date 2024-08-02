use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::FilterConf;

use super::{get_target, Filter};

pub const TYPE: &str = "eq";

struct Eq {
    field: String,
    target_value: Option<MessageValue>,
    target_field: Option<String>,
}

pub fn new(conf: FilterConf) -> Result<Box<dyn Filter>> {
    let (target_value, target_field) = get_target(&conf)?;
    Ok(Box::new(Eq {
        field: conf.field,
        target_value,
        target_field,
    }))
}

impl Filter for Eq {
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
                (MessageValue::Null, MessageValue::Null) => true,
                (MessageValue::Boolean(mv), MessageValue::Boolean(tv)) => mv == tv,
                (MessageValue::Int64(mv), MessageValue::Int64(tv)) => mv == tv,
                (MessageValue::Int64(_), MessageValue::Uint64(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Int64(_)) => todo!(),
                (MessageValue::Uint64(mv), MessageValue::Uint64(tv)) => mv == tv,
                (MessageValue::Float64(mv), MessageValue::Float64(tv)) => mv == tv,
                (MessageValue::String(mv), MessageValue::String(tv)) => mv == tv,
                (MessageValue::Bytes(_), MessageValue::Bytes(_)) => todo!(),
                (MessageValue::Array(mv), MessageValue::Array(tv)) => todo!(),
                (MessageValue::Object(_), MessageValue::Object(_)) => todo!(),
                _ => false,
            },
            None => false,
        }
    }
}