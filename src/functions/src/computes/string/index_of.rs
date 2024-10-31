use crate::computes::Computer;
use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::computer::StringItemConf;

struct IndexOf {
    field: String,
    const_value: Option<String>,
    dynamic_value: Option<String>,
    target_field: Option<String>,
}

pub fn new(conf: StringItemConf) -> Result<Box<dyn Computer>> {
    let (const_value, dynamic_value) = match conf.args {
        Some(mut args) => match args.pop() {
            Some(arg) => match get_dynamic_value_from_json(&arg) {
                common::DynamicValue::Const(value) => match value {
                    serde_json::Value::String(s) => (Some(s), None),
                    _ => bail!("只支持字符串常量"),
                },
                common::DynamicValue::Field(s) => (None, Some(s)),
            },
            None => bail!("Endswith function requires arguments"),
        },
        None => bail!("Endswith function requires arguments"),
    };
    Ok(Box::new(IndexOf {
        field: conf.field,
        const_value,
        dynamic_value,
        target_field: conf.target_field,
    }))
}

impl Computer for IndexOf {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s,
                _ => return,
            },
            None => return,
        };

        let target_value = match (&self.const_value, &self.dynamic_value) {
            (Some(value), None) => value,
            (None, Some(field)) => match message.get(field) {
                Some(mv) => match mv {
                    MessageValue::String(s) => s,
                    _ => return,
                },
                None => return,
            },
            _ => unreachable!(),
        };

        let index = match value.find(target_value) {
            Some(p) => p as i64,
            None => -1,
        };
        let resp_value = MessageValue::Int64(index);
        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), resp_value),
            None => message.set(&self.field, resp_value),
        }
    }
}
