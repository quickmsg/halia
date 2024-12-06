use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

struct Object {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Object {
        field,
        target_field,
    }))
}

impl Computer for Object {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(v) => match v {
                MessageValue::Object(_) => MessageValue::Boolean(true),
                _ => MessageValue::Boolean(false),
            },
            None => MessageValue::Boolean(false),
        };

        add_or_set_message_value!(self, message, value);
    }
}
