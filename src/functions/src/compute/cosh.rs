use anyhow::Result;
use message::Message;

use crate::computes::Computer;

struct Cosh {
    field: String,
    target_field: String,
}

fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Cosh {
        field,
        target_field,
    }))
}

impl Computer for Cosh {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).cosh()),
                ),
                message::MessageValue::Uint64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).cosh()),
                ),
                message::MessageValue::Float64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64(mv.cosh()),
                ),
                _ => {}
            },
            None => {}
        }
    }
}
