use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, args::Args, computes::Computer};

struct Array {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Array {
        field,
        target_field,
    }))
}

impl Computer for Array {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_array(&self.field) {
            Some(_) => MessageValue::Boolean(true),
            None => MessageValue::Boolean(false),
        };

        add_or_set_message_value!(self, message, value);
    }
}
