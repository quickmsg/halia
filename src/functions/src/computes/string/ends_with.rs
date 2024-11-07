use crate::{
    add_or_set_message_value,
    computes::{Arg, Computer},
};
use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

struct EndsWith {
    field: String,
    arg: Arg,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let arg = match conf.args {
        Some(mut args) => match args.pop() {
            Some(arg) => match get_dynamic_value_from_json(&arg) {
                common::DynamicValue::Const(value) => match value {
                    serde_json::Value::String(s) => Arg::Const(s),
                    _ => bail!("只支持字符串常量"),
                },
                common::DynamicValue::Field(s) => Arg::Field(s),
            },
            None => bail!("Endswith function requires arguments"),
        },
        None => bail!("Endswith function requires arguments"),
    };
    Ok(Box::new(EndsWith {
        field: conf.field,
        arg,
        target_field: conf.target_field,
    }))
}

impl Computer for EndsWith {
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

        let result = MessageValue::Boolean(value.ends_with(arg));
        add_or_set_message_value!(self, message, result);
    }
}
