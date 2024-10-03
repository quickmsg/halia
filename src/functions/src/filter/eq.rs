use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};

use super::Filter;

struct EqConst {
    field: String,
    const_value: MessageValue,
}

struct EqDynamic {
    field: String,
    target_field: String,
}

pub fn new(field: String, value: serde_json::Value) -> Result<Box<dyn Filter>> {
    match get_dynamic_value_from_json(&value) {
        common::DynamicValue::Const(value) => {
            let const_value = match value {
                serde_json::Value::Null => MessageValue::Null,
                serde_json::Value::Bool(v) => MessageValue::Boolean(v),
                serde_json::Value::Number(v) => MessageValue::from_json_number(v)?,
                serde_json::Value::String(v) => MessageValue::String(v),
                _ => bail!("不支持该类型"),
            };

            Ok(Box::new(EqConst { field, const_value }))
        }
        common::DynamicValue::Field(s) => Ok(Box::new(EqDynamic {
            field,
            target_field: s,
        })),
    }
}

impl Filter for EqConst {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(mv) => eq(mv, &self.const_value),
            None => false,
        }
    }
}

impl Filter for EqDynamic {
    fn filter(&self, msg: &Message) -> bool {
        match (msg.get(&self.field), msg.get(&self.target_field)) {
            (Some(mv), Some(tv)) => eq(mv, tv),
            _ => false,
        }
    }
}

fn eq(mv: &MessageValue, tv: &MessageValue) -> bool {
    match (mv, tv) {
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
