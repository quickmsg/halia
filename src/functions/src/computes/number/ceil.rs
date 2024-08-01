use anyhow::Result;
use message::Message;

use crate::computes::Computer;

// 最小整数
struct Ceil {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Ceil {
        field,
        target_field,
    }))
}

impl Computer for Ceil {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Float64(mv) => message.add(
                    self.target_field.clone(),
                    message::MessageValue::Float64(mv.ceil()),
                ),
                _ => {}
            },
            None => {}
        }
    }
}
