use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::FilterConfItem;

use super::Filter;

struct Lte {
    field: String,
    const_value: Option<MessageValue>,
    value_field: Option<String>,
}

pub fn new(conf: FilterConfItem) -> Result<Box<dyn Filter>> {
    match conf.value.typ {
        types::TargetValueType::Const => {
            let const_value = match conf.value.value {
                serde_json::Value::Number(v) => {
                    if let Some(v) = v.as_f64() {
                        MessageValue::Float64(v)
                    } else if let Some(v) = v.as_u64() {
                        MessageValue::Int64(v as i64)
                    } else if let Some(v) = v.as_i64() {
                        MessageValue::Int64(v)
                    } else {
                        unreachable!()
                    }
                }
                _ => todo!(),
            };

            Ok(Box::new(Lte {
                field: conf.field,
                const_value: Some(const_value),
                value_field: None,
            }))
        }
        types::TargetValueType::Variable => match conf.value.value {
            serde_json::Value::String(s) => Ok(Box::new(Lte {
                field: conf.field,
                const_value: None,
                value_field: Some(s),
            })),
            _ => unreachable!(),
        },
    }
}

impl Filter for Lte {
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
                (MessageValue::Int64(mv), MessageValue::Int64(tv)) => mv <= tv,
                (MessageValue::Float64(mv), MessageValue::Float64(tv)) => {
                    mv - tv < -1e-10 || (mv - tv).abs() == 1e-10
                }
                _ => false,
            },
            None => false,
        }
    }
}
