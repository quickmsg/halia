use anyhow::Result;
use message::Message;

use crate::computes::Computer;

// 立方根
struct Cbrt {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Cbrt {
        field,
        target_field,
    }))
}

impl Computer for Cbrt {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).cbrt()),
                ),
                message::MessageValue::Uint64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).cbrt()),
                ),
                message::MessageValue::Float64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64(mv.cbrt()),
                ),
                _ => {}
            },
            None => {}
        }
    }
}
