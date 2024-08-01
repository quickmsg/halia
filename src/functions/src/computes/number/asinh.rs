use anyhow::Result;
use message::Message;

use crate::computes::Computer;

// 反双曲正弦函数
struct Asinh {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Asinh {
        field,
        target_field,
    }))
}

impl Computer for Asinh {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => {
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64((*mv as f64).asinh()),
                    );
                }
                message::MessageValue::Uint64(mv) => {
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64((*mv as f64).asinh()),
                    );
                }
                message::MessageValue::Float64(mv) => {
                    message.add(
                        self.target_field.clone(),
                        message::MessageValue::Float64(mv.asinh()),
                    );
                }
                _ => {}
            },
            None => {}
        }
    }
}
