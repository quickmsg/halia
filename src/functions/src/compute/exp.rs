use anyhow::Result;

use crate::computes::Computer;

pub struct Exp {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Exp> {
    Ok(Exp {
        field,
        target_field,
    })
}

impl Computer for Exp {
    fn compute(&self, message: &mut message::Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).exp()),
                ),
                message::MessageValue::Uint64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).exp()),
                ),
                message::MessageValue::Float64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64(mv.exp()),
                ),
                _ => {}
            },
            None => {}
        }
    }
}
