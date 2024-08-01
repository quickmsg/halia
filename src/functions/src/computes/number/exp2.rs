use anyhow::Result;

use crate::computes::Computer;

pub struct Exp2 {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Exp2> {
    Ok(Exp2 {
        field,
        target_field,
    })
}

impl Computer for Exp2 {
    fn compute(&self, message: &mut message::Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).exp2()),
                ),
                message::MessageValue::Uint64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).exp2()),
                ),
                message::MessageValue::Float64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64(mv.exp2()),
                ),
                _ => {}
            },
            None => {}
        }
    }
}
