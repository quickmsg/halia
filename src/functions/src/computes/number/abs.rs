use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, computes::Computer, Args};

// 绝对值
struct Abs {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Abs {
        field,
        target_field,
    }))
}

impl Computer for Abs {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Int64(mv.abs()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.abs()),
                _ => return,
            },
            None => return,
        };

        add_or_set_message_value!(self, message, value);
    }
}
