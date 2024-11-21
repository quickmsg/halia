use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value, computes::Computer, get_string_field_arg, Args, StringFieldArg,
};

struct EndsWith {
    field: String,
    target_field: Option<String>,
    arg: StringFieldArg,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    let arg = get_string_field_arg(&mut args, "value")?;

    Ok(Box::new(EndsWith {
        field,
        target_field,
        arg,
    }))
}

impl Computer for EndsWith {
    fn compute(&mut self, message: &mut Message) {
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
