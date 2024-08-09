use anyhow::Result;
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};

use super::Filter;

struct CtConst {
    field: String,
    const_value: MessageValue,
}

struct CtDynamic {
    field: String,
    target_field: String,
}

pub fn new(field: String, value: serde_json::Value) -> Result<Box<dyn Filter>> {
    match get_dynamic_value_from_json(value) {
        common::DynamicValue::Const(value) => Ok(Box::new(CtConst {
            field,
            const_value: MessageValue::from(value),
        })),
        common::DynamicValue::Field(s) => Ok(Box::new(CtDynamic {
            field,
            target_field: s,
        })),
    }
}

impl Filter for CtConst {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(mv) => ct(mv, &self.const_value),
            None => false,
        }
    }
}

impl Filter for CtDynamic {
    fn filter(&self, msg: &Message) -> bool {
        match (msg.get(&self.field), msg.get(&self.target_field)) {
            (Some(mv), Some(tv)) => ct(mv, tv),
            _ => false,
        }
    }
}

fn ct(mv: &MessageValue, tv: &MessageValue) -> bool {
    match (mv, tv) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contain_const_bool() {
        let ct_const = CtConst {
            field: "a".to_string(),
            const_value: MessageValue::Boolean(true),
        };

        let msg = r#"{"a":[false, false, true]}"#;
        let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
        let msg = Message::from(msg);
        assert_eq!(ct_const.filter(&msg), true);

        let msg = r#"{"a":[false, false, false]}"#;
        let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
        let msg = Message::from(msg);
        assert_eq!(ct_const.filter(&msg), false);
    }

    #[test]
    fn contain_const_float() {
        let ct_const = CtConst {
            field: "a".to_string(),
            const_value: MessageValue::Float64(1.1),
        };

        let msg = r#"{"a":[1.1,2.2,3.3]}"#;
        let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
        let msg = Message::from(msg);
        assert_eq!(ct_const.filter(&msg), true);

        let msg = r#"{"a":[2.2,3.3]}"#;
        let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
        let msg = Message::from(msg);
        assert_eq!(ct_const.filter(&msg), false);
    }

    #[test]
    fn contain_const_string() {
        let ct_const = CtConst {
            field: "a".to_string(),
            const_value: MessageValue::String("bbbbb".to_owned()),
        };

        let msg = r#"{"a":"abbbbbadd"}"#;
        let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
        let msg = Message::from(msg);
        assert_eq!(ct_const.filter(&msg), true);

        let msg = r#"{"a":"abbbbadd"}"#;
        let msg: serde_json::Value = serde_json::from_str(msg).unwrap();
        let msg = Message::from(msg);
        assert_eq!(ct_const.filter(&msg), false);
    }

    #[test]
    fn contain_dynamic() {}
}
