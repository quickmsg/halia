use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::FilterConf;

use super::Filter;

struct Gt {
    field: String,
    const_value: Option<MessageValue>,
    value_field: Option<String>,
}

pub fn new(conf: FilterConf) -> Result<Box<dyn Filter>> {
    match conf.value.typ {
        types::TargetValueType::Const => {
            let const_value = match conf.value.value {
                serde_json::Value::Null => MessageValue::Null,
                serde_json::Value::Bool(v) => MessageValue::Boolean(v),
                serde_json::Value::Number(v) => {
                    if let Some(v) = v.as_f64() {
                        MessageValue::Float64(v)
                    } else if let Some(v) = v.as_u64() {
                        MessageValue::Uint64(v)
                    } else if let Some(v) = v.as_i64() {
                        MessageValue::Int64(v)
                    } else {
                        unreachable!()
                    }
                }
                serde_json::Value::String(v) => MessageValue::String(v),
                // serde_json::Value::Array(v) => MessageValue::Array(v),
                // serde_json::Value::Object(v) => MessageValue::Object(v),
                _ => todo!(),
            };

            Ok(Box::new(Gt {
                field: conf.field,
                const_value: Some(const_value),
                value_field: None,
            }))
        }
        types::TargetValueType::Variable => match conf.value.value {
            serde_json::Value::String(s) => Ok(Box::new(Gt {
                field: conf.field,
                const_value: None,
                value_field: Some(s),
            })),
            _ => unreachable!(),
        },
    }
}

impl Filter for Gt {
    fn filter(&self, msg: &Message) -> bool {
        let target_value = {
            if let Some(target_value) = &self.const_value {
                target_value
            } else if let Some(target_field) = &self.value_field {
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
                (MessageValue::Int64(mv), MessageValue::Uint64(tv)) => *mv >= *tv as i64,
                (MessageValue::Uint64(mv), MessageValue::Int64(tv)) => *mv >= *tv as u64,
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
