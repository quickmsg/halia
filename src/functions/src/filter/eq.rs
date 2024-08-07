use anyhow::{bail, Result};
use message::{Message, MessageValue};
use types::rules::functions::FilterConfItem;

use crate::get_target_value;

use super::Filter;

struct Eq {
    field: String,
    const_value: Option<MessageValue>,
    value_field: Option<String>,
}

pub fn new(conf: FilterConfItem) -> Result<Box<dyn Filter>> {
    match conf.value.typ {
        types::TargetValueType::Const => {
            let const_value = match conf.value.value {
                serde_json::Value::Null => MessageValue::Null,
                serde_json::Value::Bool(v) => MessageValue::Boolean(v),
                serde_json::Value::Number(v) => MessageValue::from_json_number(v)?,
                serde_json::Value::String(v) => MessageValue::String(v),
                _ => bail!("不支持该类型"),
            };

            Ok(Box::new(Eq {
                field: conf.field,
                const_value: Some(const_value),
                value_field: None,
            }))
        }
        types::TargetValueType::Variable => match conf.value.value {
            serde_json::Value::String(s) => Ok(Box::new(Eq {
                field: conf.field,
                const_value: None,
                value_field: Some(s),
            })),
            _ => bail!("变量字段名称必须为字符串变量"),
        },
    }
}

impl Filter for Eq {
    fn filter(&self, msg: &Message) -> bool {
        let target_value = get_target_value!(self, msg);

        match msg.get(&self.field) {
            Some(message_value) => match (message_value, target_value) {
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
            },
            None => false,
        }
    }
}
