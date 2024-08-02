use anyhow::Result;
use message::Message;

use crate::computes::Computer;

struct Cos {
    field: String,
}

pub fn new(field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Cos { field }))
}

impl Computer for Cos {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => message.add(
                    self.field.clone(),
                    message::MessageValue::Float64((*mv as f64).cos()),
                ),
                message::MessageValue::Uint64(mv) => message.add(
                    self.field.clone(),
                    message::MessageValue::Float64((*mv as f64).cos()),
                ),
                message::MessageValue::Float64(mv) => {
                    message.add(self.field.clone(), message::MessageValue::Float64(mv.cos()))
                }
                _ => {}
            },
            None => {}
        }
    }
}
