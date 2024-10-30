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

        match value {
            MessageValue::String(mv) => match target_value {
                MessageValue::String(tv) => mv.contains(tv),
                _ => false,
            },
            MessageValue::Bytes(mv) => match target_value {
                // TODO
                MessageValue::Bytes(tv) => {
                    //  mv.contains(tv);
                    true
                }
                _ => false,
            },
            MessageValue::Array(mv) => match target_value {
                MessageValue::Null => mv.iter().find(|v| **v == MessageValue::Null).is_some(),
                MessageValue::Boolean(b) => mv
                    .iter()
                    .find(|v| match v {
                        MessageValue::Boolean(v) => v == b,
                        _ => false,
                    })
                    .is_some(),
                MessageValue::Int64(i) => mv
                    .iter()
                    .find(|v| match v {
                        MessageValue::Int64(v) => v == i,
                        _ => false,
                    })
                    .is_some(),
                MessageValue::Float64(f) => mv
                    .iter()
                    .find(|v| match v {
                        MessageValue::Float64(v) => v == f,
                        _ => false,
                    })
                    .is_some(),
                MessageValue::String(s) => mv
                    .iter()
                    .find(|v| match v {
                        MessageValue::String(v) => v == s,
                        _ => false,
                    })
                    .is_some(),
                MessageValue::Bytes(vec) => mv
                    .iter()
                    .find(|v| match v {
                        MessageValue::Bytes(v) => v == vec,
                        _ => false,
                    })
                    .is_some(),
                MessageValue::Array(vec) => mv
                    .iter()
                    .find(|v| match v {
                        MessageValue::Array(v) => v == vec,
                        _ => false,
                    })
                    .is_some(),
                MessageValue::Object(hash_map) => mv
                    .iter()
                    .find(|v| match v {
                        MessageValue::Object(v) => v == hash_map,
                        _ => false,
                    })
                    .is_some(),
            },
            MessageValue::Object(hash_map) => todo!(),
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
