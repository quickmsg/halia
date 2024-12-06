use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, nodes::{args::Args, computes::Computer}};

struct Int {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Int {
        field,
        target_field,
    }))
}

impl Computer for Int {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(v) => match v {
                MessageValue::Boolean(b) => {
                    if *b {
                        MessageValue::Int64(1)
                    } else {
                        MessageValue::Int64(0)
                    }
                }
                MessageValue::Int64(i) => MessageValue::Int64(*i),
                MessageValue::Float64(f) => MessageValue::Int64(*f as i64),
                MessageValue::String(s) => match s.parse::<i64>() {
                    Ok(num) => MessageValue::Int64(num),
                    Err(_) => MessageValue::Null,
                },
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}
