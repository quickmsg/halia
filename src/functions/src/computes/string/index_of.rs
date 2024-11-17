use crate::computes::{Arg, Computer};
use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

struct IndexOf {
    field: String,
    target_field: Option<String>,
    arg: Arg,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let arg = conf
        .args
        .as_ref()
        .and_then(|conf_args| conf_args.get("value"))
        .ok_or_else(|| anyhow::anyhow!("Endswith function requires value arguments"))?;

    let arg = match get_dynamic_value_from_json(arg) {
        common::DynamicValue::Const(serde_json::Value::String(s)) => Arg::Const(s),
        common::DynamicValue::Const(_) => bail!("只支持字符串常量"),
        common::DynamicValue::Field(f) => Arg::Field(f),
    };

    Ok(Box::new(IndexOf {
        field: conf.field,
        target_field: conf.target_field,
        arg,
    }))
}

impl Computer for IndexOf {
    fn compute(&self, message: &mut Message) {
        let value = message.get(&self.field).and_then(|mv| match mv {
            MessageValue::String(s) => Some(s),
            _ => None,
        });

        let value = match value {
            Some(s) => s,
            None => return,
        };

        let target_value = match &self.arg {
            Arg::Const(s) => s,
            Arg::Field(f) => match message.get(f) {
                Some(mv) => match mv {
                    MessageValue::String(s) => s,
                    _ => return,
                },
                None => return,
            },
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
