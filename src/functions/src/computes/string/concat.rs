use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value, computes::Computer, get_array_string_field_arg, Args, StringFieldArg,
};

struct Concat {
    field: String,
    args: Vec<StringFieldArg>,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    let args = get_array_string_field_arg(&mut args, "value")?;

    Ok(Box::new(Concat {
        field,
        target_field,
        args,
    }))
}

impl Computer for Concat {
    fn compute(&mut self, message: &mut Message) {
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
                StringFieldArg::Const(s) => result.push_str(s),
                StringFieldArg::Field(field) => match message.get(field) {
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
