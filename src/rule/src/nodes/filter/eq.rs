use anyhow::Result;
use async_trait::async_trait;
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use tracing::debug;
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

#[async_trait]
impl Filter for Eq {
    async fn filter(&self, msg: &Message) -> bool {
        let value = match msg.get(&self.field) {
            Some(value) => value,
            None => return false,
        };

        debug!("value: {:?}", value);
        let target_value = match (&self.const_value, &self.target_field) {
            (Some(const_value), None) => const_value,
            (None, Some(target_field)) => match msg.get(&target_field) {
                Some(target_value) => target_value,
                None => return false,
            },
            _ => unreachable!(),
        };

        debug!("target_value: {:?}", target_value);


        value == target_value
    }
}
