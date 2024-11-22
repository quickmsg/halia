use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, computes::Computer, Args};

struct Reverse {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    Ok(Box::new(Reverse {
        field,
        target_field,
    }))
}

impl Computer for Reverse {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(mv) => mv,
            None => return,
        };

        let result = value.chars().rev().collect::<String>();

        add_or_set_message_value!(self, message, MessageValue::String(result));
    }
}
