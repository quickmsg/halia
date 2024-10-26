use anyhow::Result;
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::filter::ItemConf;

use super::Filter;

struct Ct {
    field: String,
    const_value: Option<MessageValue>,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Filter>> {
    let (const_value, target_field) = match get_dynamic_value_from_json(&conf.value) {
        common::DynamicValue::Const(value) => (Some(MessageValue::from(value)), None),
        common::DynamicValue::Field(s) => (None, Some(s)),
    };

    Ok(Box::new(Ct {
        field: conf.field,
        const_value,
        target_field,
    }))
}

impl Filter for Ct {
    fn filter(&self, msg: &Message) -> bool {
        let value = match msg.get(&self.field) {
            Some(value) => value,
            None => return false,
        };

        let target_value = match (&self.const_value, &self.target_field) {
            (None, Some(target_field)) => match msg.get(target_field) {
                Some(target_value) => target_value,
                None => return false,
            },
            (Some(value), None) => value,
            _ => unreachable!(),
        };

        match (value, target_value) {
            (MessageValue::String(mv), MessageValue::String(tv)) => mv.contains(tv),
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
            (MessageValue::Array(_mv), MessageValue::Array(_tv)) => {
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
                false
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
        }
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[test]
    // fn contain_const_bool() {
    //     let ct_const = CtConst {
    //         field: "a".to_string(),
    //         const_value: MessageValue::Boolean(true),
    //     };

    //     let msg = r#"{"a":[false, false, true]}"#;
    //     let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
    //     let msg = Message::from(msg);
    //     assert_eq!(ct_const.filter(&msg), true);

    //     let msg = r#"{"a":[false, false, false]}"#;
    //     let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
    //     let msg = Message::from(msg);
    //     assert_eq!(ct_const.filter(&msg), false);
    // }

    // #[test]
    // fn contain_const_float() {
    //     let ct_const = CtConst {
    //         field: "a".to_string(),
    //         const_value: MessageValue::Float64(1.1),
    //     };

    //     let msg = r#"{"a":[1.1,2.2,3.3]}"#;
    //     let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
    //     let msg = Message::from(msg);
    //     assert_eq!(ct_const.filter(&msg), true);

    //     let msg = r#"{"a":[2.2,3.3]}"#;
    //     let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
    //     let msg = Message::from(msg);
    //     assert_eq!(ct_const.filter(&msg), false);
    // }

    // #[test]
    // fn contain_const_string() {
    //     let ct_const = CtConst {
    //         field: "a".to_string(),
    //         const_value: MessageValue::String("bbbbb".to_owned()),
    //     };

    //     let msg = r#"{"a":"abbbbbadd"}"#;
    //     let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
    //     let msg = Message::from(msg);
    //     assert_eq!(ct_const.filter(&msg), true);

    //     let msg = r#"{"a":"abbbbadd"}"#;
    //     let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
    //     let msg = Message::from(msg);
    //     assert_eq!(ct_const.filter(&msg), false);
    // }

    // #[test]
    // fn contain_dynamic() {}
}
