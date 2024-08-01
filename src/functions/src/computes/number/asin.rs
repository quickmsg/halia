use anyhow::Result;
use message::Message;

use crate::computes::Computer;

// 反正弦函数
struct Asin {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Asin {
        field,
        target_field,
    }))
}

impl Computer for Asin {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(value) => match value {
                message::MessageValue::Int64(mv) => {
                    if *mv < 1 || *mv > 1 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64((*mv as f64).asin()),
                    )
                }
                message::MessageValue::Uint64(mv) => {
                    if *mv > 1 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64((*mv as f64).asin()),
                    )
                }
                message::MessageValue::Float64(mv) => {
                    if *mv < -1.0 || *mv > 1.0 {
                        return;
                    }
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64(mv.asin()),
                    )
                }
                _ => {}
            },
            None => {}
        }
    }
}
