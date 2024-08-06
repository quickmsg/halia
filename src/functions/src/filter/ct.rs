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
                (MessageValue::String(mv), MessageValue::String(tv)) => mv == tv,
                (MessageValue::Bytes(_), MessageValue::Bytes(_)) => todo!(),
                (MessageValue::Array(mv), MessageValue::Array(tv)) => todo!(),
                (MessageValue::Object(_), MessageValue::Object(_)) => todo!(),
                _ => false,
            },
            None => false,
        }
    }
}
