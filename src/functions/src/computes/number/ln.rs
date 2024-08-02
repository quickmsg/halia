use anyhow::Result;

use crate::computes::Computer;

struct Ln {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Ln {
        field,
        target_field,
    }))
}

impl Computer for Ln {
    fn compute(&self, message: &mut message::Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => {
                    if *mv <= 0 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64((*mv as f64).ln()),
                    )
                }
                message::MessageValue::Uint64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64((*mv as f64).ln()),
                ),
                message::MessageValue::Float64(mv) => {
                    if *mv <= 0.0 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64(mv.ln()),
                    )
                }
                _ => {}
            },
            None => {}
        }
    }
}