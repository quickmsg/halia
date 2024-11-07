use crate::computes::Computer;
use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use regex::Regex;
use types::rules::functions::computer::ItemConf;

struct RegexMatch {
    field: String,
    const_value: Option<Regex>,
    dynamic_value: Option<String>,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let (const_value, dynamic_value) = match conf.args {
        Some(mut args) => match args.pop() {
            Some(arg) => match get_dynamic_value_from_json(&arg) {
                common::DynamicValue::Const(value) => match value {
                    serde_json::Value::String(s) => {
                        let reg = Regex::new(&s)?;
                        (Some(reg), None)
                    }
                    _ => bail!("只支持字符串常量"),
                },
                common::DynamicValue::Field(_) => todo!(),
            },
            None => bail!("Endswith function requires arguments"),
        },
        None => bail!("Endswith function requires arguments"),
    };
    Ok(Box::new(RegexMatch {
        field: conf.field,
        const_value,
        dynamic_value,
        target_field: conf.target_field,
    }))
}

impl Computer for RegexMatch {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s,
                _ => return,
            },
            None => return,
        };

        let resp_value = match (&self.const_value, &self.dynamic_value) {
            (Some(reg), None) => reg.find(&value).is_some(),
            (None, Some(field)) => match message.get(field) {
                Some(mv) => match mv {
                    MessageValue::String(s) => match Regex::new(&s) {
                        Ok(reg) => reg.find(&value).is_some(),
                        Err(_) => return,
                    },
                    _ => return,
                },
                None => return,
            },
            _ => unreachable!(),
        };

        let resp_value = MessageValue::Boolean(resp_value);

        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), resp_value),
            None => message.set(&self.field, resp_value),
        }
    }
}
