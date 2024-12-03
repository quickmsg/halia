use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, computes::Computer, Args};

struct Len {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Len {
        field,
        target_field,
    }))
}

impl Computer for Len {
    fn compute(&mut self, message: &mut Message) {
        let result = match message.get_array(&self.field) {
            Some(arr) => arr.len(),
            None => return,
        };
        add_or_set_message_value!(self, message, MessageValue::Int64(result as i64));
    }
}
