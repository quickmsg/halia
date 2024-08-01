use anyhow::Result;

use crate::computes::Computer;

struct Floor {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Floor {
        field,
        target_field,
    }))
}

impl Computer for Floor {
    fn compute(&self, message: &mut message::Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Float64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64(mv.floor()),
                ),
                _ => {}
            },
            None => {}
        }
    }
}
