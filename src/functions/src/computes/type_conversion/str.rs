use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, args::Args, computes::Computer};

struct Str {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Str {
        field,
        target_field,
    }))
}

impl Computer for Str {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(v) => match v {
                MessageValue::Boolean(b) => {
                    if *b {
                        MessageValue::String("true".to_string())
                    } else {
                        MessageValue::String("false".to_string())
                    }
                }
                MessageValue::Int64(i) => MessageValue::String(i.to_string()),
                MessageValue::Float64(f) => MessageValue::String(f.to_string()),
                MessageValue::String(s) => MessageValue::String(s.clone()),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}
