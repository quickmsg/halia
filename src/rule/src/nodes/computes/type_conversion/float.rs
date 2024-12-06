use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, nodes::{args::Args, computes::Computer}};

struct Float {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Float {
        field,
        target_field,
    }))
}

impl Computer for Float {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(v) => match v {
                MessageValue::Boolean(b) => {
                    if *b {
                        MessageValue::Float64(1.0)
                    } else {
                        MessageValue::Float64(0.0)
                    }
                }
                MessageValue::Int64(i) => MessageValue::Float64(*i as f64),
                MessageValue::Float64(f) => MessageValue::Float64(*f),
                MessageValue::String(s) => match s.parse::<f64>() {
                    Ok(num) => MessageValue::Float64(num),
                    Err(_) => MessageValue::Null,
                },
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}
