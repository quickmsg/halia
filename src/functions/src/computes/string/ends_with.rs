use crate::{add_or_set_message_value, computes::Computer, StringArg};
use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

use super::get_string_arg;

struct EndsWith {
    field: String,
    arg: StringArg,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let arg = get_string_arg(&conf, "value")?;

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
            StringArg::Const(s) => s,
            StringArg::Field(f) => match message.get(f) {
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
