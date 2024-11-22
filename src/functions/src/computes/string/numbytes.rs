use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, computes::Computer, Args};

// TODO 搞清楚具体需求
struct Numbytes {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    Ok(Box::new(Numbytes {
        field,
        target_field,
    }))
}

impl Computer for Numbytes {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(mv) => mv,
            None => return,
        };

        let result = value.len() as i64;
        add_or_set_message_value!(self, message, MessageValue::Int64(result));
    }
}