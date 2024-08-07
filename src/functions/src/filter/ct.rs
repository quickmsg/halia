use anyhow::{bail, Result};
use message::{Message, MessageValue};
use types::rules::functions::FilterConfItem;

use crate::get_target_value;

use super::Filter;

struct Ct {
    field: String,
    const_value: Option<MessageValue>,
    value_field: Option<String>,
}

pub fn new(conf: FilterConfItem) -> Result<Box<dyn Filter>> {
    match conf.value.typ {
        types::TargetValueType::Const => {
            let const_value = MessageValue::from(conf.value.value);

            Ok(Box::new(Ct {
                field: conf.field,
                const_value: Some(const_value),
                value_field: None,
            }))
        }
        types::TargetValueType::Variable => match conf.value.value {
            serde_json::Value::String(s) => Ok(Box::new(Ct {
                field: conf.field,
                const_value: None,
                value_field: Some(s),
            })),
            _ => bail!("变量字段名称必须为字符串变量"),
        },
    }
}

impl Filter for Ct {
    // TODO
    fn filter(&self, msg: &Message) -> bool {
        let target_value = get_target_value!(self, msg);
        match msg.get(&self.field) {
            Some(message_value) => match (message_value, target_value) {
                (MessageValue::String(_), MessageValue::String(_)) => todo!(),

                (MessageValue::Array(mv), MessageValue::Null) => {
                    for v in mv {
                        match v {
                            MessageValue::Null => return true,
                            _ => {}
                        }
                    }
                    false
                }
                (MessageValue::Array(mv), MessageValue::Boolean(tv)) => {
                    for v in mv {
                        match v {
                            MessageValue::Boolean(value) => {
                                if value == tv {
                                    return true;
                                }
                            }
                            _ => {}
                        }
                    }
                    false
                }
                (MessageValue::Array(mv), MessageValue::Int64(tv)) => {
                    for v in mv {
                        match v {
                            MessageValue::Int64(value) => {
                                if value == tv {
                                    return true;
                                }
                            }
                            _ => {}
                        }
                    }
                    false
                }
                (MessageValue::Array(mv), MessageValue::Float64(tv)) => {
                    for v in mv {
                        match v {
                            MessageValue::Float64(value) => {
                                if (value - tv).abs() < 1e-10 {
                                    return true;
                                }
                            }
                            _ => {}
                        }
                    }
                    false
                }
                (MessageValue::Array(mv), MessageValue::String(tv)) => {
                    for v in mv {
                        match v {
                            MessageValue::String(value) => {
                                if value == tv {
                                    return true;
                                }
                            }
                            _ => {}
                        }
                    }
                    false
                }
                (MessageValue::Array(mv), MessageValue::Bytes(tv)) => {
                    for v in mv {
                        match v {
                            MessageValue::Bytes(value) => {
                                if value == tv {
                                    return true;
                                }
                            }
                            _ => {}
                        }
                    }
                    false
                }
                (MessageValue::Array(mv), MessageValue::Array(tv)) => {
                    // for v in mv {
                    //     match v {
                    //         MessageValue::Array(value) => {
                    //             if value == tv {
                    //                 return true;
                    //             }
                    //         }
                    //         _ => {}
                    //     }
                    // }
                    // false
                    todo!()
                }
                (MessageValue::Array(_), MessageValue::Object(_)) => todo!(),

                (MessageValue::Object(_), MessageValue::Null) => todo!(),
                (MessageValue::Object(_), MessageValue::Boolean(_)) => todo!(),
                (MessageValue::Object(_), MessageValue::Int64(_)) => todo!(),
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
