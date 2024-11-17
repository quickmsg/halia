use crate::{
    add_or_set_message_value,
    computes::{Arg, Computer},
};
use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

struct Includes {
    field: String,
    arg: Arg,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let arg = conf
        .args
        .as_ref()
        .and_then(|conf_args| conf_args.get("value"))
        .ok_or_else(|| anyhow::anyhow!("Includes function requires value arguments"))?;
    let arg = match get_dynamic_value_from_json(arg) {
        common::DynamicValue::Const(value) => match value {
            serde_json::Value::String(s) => Arg::Const(s),
            _ => bail!("只支持字符串常量"),
        },
        common::DynamicValue::Field(s) => Arg::Field(s),
    };
    Ok(Box::new(Includes {
        field: conf.field,
        arg,
        target_field: conf.target_field,
    }))
}

impl Computer for Includes {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s,
                _ => return,
            },
            None => return,
        };

        let arg = match &self.arg {
            Arg::Const(s) => s,
            Arg::Field(f) => match message.get(f) {
                Some(mv) => match mv {
                    MessageValue::String(s) => s,
                    _ => return,
                },
                None => return,
            },
        };

        let result = MessageValue::Boolean(value.contains(arg));
        add_or_set_message_value!(self, message, result);
    }
}
