use crate::{add_or_set_message_value, computes::Computer, get_string_field_arg, StringFieldArg};
use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ItemConf;

struct EndsWith {
    field: String,
    arg: StringFieldArg,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let arg = get_string_field_arg(&conf, "value")?;

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
            StringFieldArg::Const(s) => s,
            StringFieldArg::Field(f) => match message.get(f) {
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
