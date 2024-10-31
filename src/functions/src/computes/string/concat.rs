use crate::{
    add_or_set_message_value,
    computes::{Arg, Computer},
};
use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::computer::StringItemConf;

struct Concat {
    field: String,
    args: Vec<Arg>,
    target_field: Option<String>,
}

pub fn new(conf: StringItemConf) -> Result<Box<dyn Computer>> {
    let args = match conf.args {
        Some(conf_args) => {
            let mut args = Vec::with_capacity(conf_args.len());
            for conf_arg in conf_args {
                let arg = match get_dynamic_value_from_json(&conf_arg) {
                    common::DynamicValue::Const(value) => match value {
                        serde_json::Value::String(s) => Arg::Const(s),
                        _ => bail!("只支持字符串常量"),
                    },
                    common::DynamicValue::Field(field) => Arg::Field(field),
                };
                args.push(arg);
            }
            args
        }
        None => bail!("Endswith function requires arguments"),
    };

    Ok(Box::new(Concat {
        field: conf.field,
        args,
        target_field: conf.target_field,
    }))
}

impl Computer for Concat {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s,
                _ => return,
            },
            None => return,
        };

        let mut result = value.clone();

        for arg in &self.args {
            match arg {
                Arg::Const(s) => result.push_str(s),
                Arg::Field(field) => match message.get(field) {
                    Some(mv) => match mv {
                        MessageValue::String(s) => result.push_str(s),
                        _ => return,
                    },
                    None => return,
                },
            }
        }

        let result = MessageValue::String(result);
        add_or_set_message_value!(self, message, result);
    }
}
