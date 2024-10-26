use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::filter::ItemConf;

use super::Filter;

struct Gte {
    field: String,
    const_value: Option<MessageValue>,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Filter>> {
    match get_dynamic_value_from_json(&conf.value) {
        common::DynamicValue::Const(value) => {
            let const_value = match value {
                serde_json::Value::Number(v) => MessageValue::from_json_number(v)?,
                _ => bail!("不支持该类型"),
            };

            Ok(Box::new(Gte {
                field: conf.field,
                const_value: Some(const_value),
                target_field: None,
            }))
        }
        common::DynamicValue::Field(s) => Ok(Box::new(Gte {
            field: conf.field,
            target_field: Some(s),
            const_value: None,
        })),
    }
}

impl Filter for Gte {
    fn filter(&self, msg: &Message) -> bool {
        let value = match msg.get(&self.field) {
            Some(value) => value,
            None => return false,
        };

        let target_value = match (&self.const_value, &self.target_field) {
            (Some(value), None) => value,
            (None, Some(target_field)) => match msg.get(target_field) {
                Some(target_value) => target_value,
                None => return false,
            },
            _ => unreachable!(),
        };
        match (value, target_value) {
            (MessageValue::Int64(mv), MessageValue::Int64(tv)) => mv >= tv,
            (MessageValue::Float64(mv), MessageValue::Float64(tv)) => {
                mv - tv > 1e-10 || (mv - tv).abs() < 1e-10
            }
            _ => false,
        }
    }
}
