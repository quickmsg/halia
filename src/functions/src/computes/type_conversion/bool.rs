use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, args::Args, computes::Computer};

struct Bool {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Bool {
        field,
        target_field,
    }))
}

impl Computer for Bool {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(v) => match v {
                MessageValue::Boolean(b) => MessageValue::Boolean(*b),
                MessageValue::Int64(i) => {
                    if *i == 0 {
                        MessageValue::Boolean(false)
                    } else if *i == 1 {
                        MessageValue::Boolean(true)
                    } else {
                        MessageValue::Null
                    }
                }
                MessageValue::Float64(f) => {
                    if *f == 0.0 {
                        MessageValue::Boolean(false)
                    } else if *f == 1.0 {
                        MessageValue::Boolean(true)
                    } else {
                        MessageValue::Null
                    }
                }
                MessageValue::String(s) => {
                    if *s == "true" {
                        MessageValue::Boolean(true)
                    } else if *s == "false" {
                        MessageValue::Boolean(false)
                    } else {
                        MessageValue::Null
                    }
                }
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}
