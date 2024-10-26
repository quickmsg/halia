use anyhow::Result;
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::filter::ItemConf;

use super::Filter;

struct Eq {
    field: String,
    const_value: Option<MessageValue>,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Filter>> {
    let (const_value, target_field) = match get_dynamic_value_from_json(&conf.value) {
        common::DynamicValue::Const(value) => {
            let const_value = match value {
                serde_json::Value::Null => MessageValue::Null,
                serde_json::Value::Bool(v) => MessageValue::Boolean(v),
                serde_json::Value::Number(v) => MessageValue::from_json_number(v)?,
                serde_json::Value::String(v) => MessageValue::String(v),
                serde_json::Value::Array(_vec) => todo!(),
                serde_json::Value::Object(_map) => todo!(),
            };

            (Some(const_value), None)
        }
        common::DynamicValue::Field(s) => (None, Some(s)),
    };

    Ok(Box::new(Eq {
        field: conf.field,
        const_value,
        target_field,
    }))
}

impl Filter for Eq {
    fn filter(&self, msg: &Message) -> bool {
        let value = match msg.get(&self.field) {
            Some(value) => value,
            None => return false,
        };
        let target_value = match (&self.const_value, &self.target_field) {
            (Some(const_value), None) => const_value,
            (None, Some(target_field)) => match msg.get(&target_field) {
                Some(target_value) => target_value,
                None => return false,
            },
            _ => unreachable!(),
        };

        match (value, target_value) {
            (MessageValue::Null, MessageValue::Null) => true,
            (MessageValue::Boolean(mv), MessageValue::Boolean(tv)) => mv == tv,
            (MessageValue::Int64(mv), MessageValue::Int64(tv)) => mv == tv,
            (MessageValue::Float64(mv), MessageValue::Float64(tv)) => (mv - tv).abs() < 1e-10,
            (MessageValue::String(mv), MessageValue::String(tv)) => mv == tv,
            (MessageValue::Bytes(mv), MessageValue::Bytes(tv)) => mv == tv,
            (MessageValue::Array(mv), MessageValue::Array(tv)) => {
                if mv.len() != tv.len() {
                    false
                } else {
                    for i in 0..mv.len() {
                        match (&mv[i], &tv[i]) {
                            (MessageValue::Null, MessageValue::Null) => {}
                            (MessageValue::Boolean(mv), MessageValue::Boolean(tv)) => {
                                if mv != tv {
                                    return false;
                                }
                            }
                            (MessageValue::Int64(mv), MessageValue::Int64(tv)) => {
                                if mv != tv {
                                    return false;
                                }
                            }
                            (MessageValue::Float64(mv), MessageValue::Float64(tv)) => {
                                if mv != tv {
                                    return false;
                                }
                            }
                            (MessageValue::String(mv), MessageValue::String(tv)) => {
                                if mv != tv {
                                    return false;
                                }
                            }
                            (MessageValue::Bytes(_), MessageValue::Null) => todo!(),
                            (MessageValue::Bytes(_), MessageValue::Boolean(_)) => todo!(),
                            (MessageValue::Bytes(_), MessageValue::Int64(_)) => todo!(),
                            (MessageValue::Bytes(_), MessageValue::Float64(_)) => todo!(),
                            (MessageValue::Bytes(_), MessageValue::String(_)) => todo!(),
                            (MessageValue::Bytes(_), MessageValue::Bytes(_)) => todo!(),
                            (MessageValue::Bytes(_), MessageValue::Array(_)) => todo!(),
                            (MessageValue::Bytes(_), MessageValue::Object(_)) => todo!(),
                            (MessageValue::Array(_), MessageValue::Null) => todo!(),
                            (MessageValue::Array(_), MessageValue::Boolean(_)) => todo!(),
                            (MessageValue::Array(_), MessageValue::Int64(_)) => todo!(),
                            (MessageValue::Array(_), MessageValue::Float64(_)) => todo!(),
                            (MessageValue::Array(_), MessageValue::String(_)) => todo!(),
                            (MessageValue::Array(_), MessageValue::Bytes(_)) => todo!(),
                            (MessageValue::Array(_), MessageValue::Array(_)) => todo!(),
                            (MessageValue::Array(_), MessageValue::Object(_)) => todo!(),
                            (MessageValue::Object(_), MessageValue::Null) => todo!(),
                            (MessageValue::Object(_), MessageValue::Boolean(_)) => todo!(),
                            (MessageValue::Object(_), MessageValue::Int64(_)) => todo!(),
                            (MessageValue::Object(_), MessageValue::Float64(_)) => todo!(),
                            (MessageValue::Object(_), MessageValue::String(_)) => todo!(),
                            (MessageValue::Object(_), MessageValue::Bytes(_)) => todo!(),
                            (MessageValue::Object(_), MessageValue::Array(_)) => todo!(),
                            (MessageValue::Object(_), MessageValue::Object(_)) => todo!(),
                            _ => return false,
                        }
                    }

                    true
                }
            }
            // (MessageValue::Object(_), MessageValue::Object(_)) => todo!(),
            _ => false,
        }
    }
}
