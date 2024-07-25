use anyhow::Result;
use message::{Message, MessageValue};
use serde::{Deserialize, Serialize};
use types::rules::functions::FilterConf;

use super::{get_target, Filter};

struct Gt {
    field: String,
    target_value: Option<MessageValue>,
    target_field: Option<String>,
}

pub const TYPE: &str = "gt";

pub fn new(conf: FilterConf) -> Result<Box<dyn Filter>> {
    let (target_value, target_field) = get_target(&conf)?;
    Ok(Box::new(Gt {
        field: conf.field,
        target_value,
        target_field,
    }))
}

#[derive(Deserialize, Serialize)]
struct Conf {
    field: String,
    value: serde_json::Value,
}

impl Filter for Gt {
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
                (MessageValue::Int64(message_value), MessageValue::Int64(target_value)) => {
                    message_value >= target_value
                }
                (MessageValue::Int64(_), MessageValue::Uint64(_)) => todo!(),
                (MessageValue::Uint64(_), MessageValue::Int64(_)) => todo!(),
                (MessageValue::Uint64(message_value), MessageValue::Uint64(target_value)) => {
                    message_value >= target_value
                }
                (MessageValue::Float64(message_value), MessageValue::Float64(target_value)) => {
                    message_value >= target_value
                }
                _ => false,
            },
            None => false,
        }
    }
}
