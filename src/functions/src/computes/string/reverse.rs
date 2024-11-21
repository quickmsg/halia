use anyhow::Result;
use message::{Message, MessageValue};

use crate::{computes::Computer, Args};

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
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(s) => MessageValue::Int64(s.len() as i64),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), value),
            None => message.set(&self.field, value),
        }
    }
}
