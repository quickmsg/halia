use anyhow::Result;
use message::MessageValue;

use crate::{computes::Computer, Args};

struct Ln {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Ln {
        field,
        target_field,
    }))
}

impl Computer for Ln {
    fn compute(&mut self, message: &mut message::Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => {
                    if *mv <= 0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64((*mv as f64).ln())
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv <= 0.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64(mv.ln())
                    }
                }
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
