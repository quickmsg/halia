use crate::{add_or_set_message_value, computes::Computer, Args};
use anyhow::Result;
use message::{Message, MessageValue};

struct Upper {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    Ok(Box::new(Upper {
        field,
        target_field,
    }))
}

impl Computer for Upper {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(s) => MessageValue::String(s.to_uppercase()),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}
