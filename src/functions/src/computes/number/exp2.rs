use anyhow::Result;
use message::MessageValue;

use crate::{computes::Computer, Args};

pub struct Exp2 {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Exp2 {
        field,
        target_field,
    }))
}

impl Computer for Exp2 {
    fn compute(&mut self, message: &mut message::Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Float64((*mv as f64).exp2()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.exp2()),
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
