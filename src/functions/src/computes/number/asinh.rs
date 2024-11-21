use anyhow::Result;
use message::{Message, MessageValue};

use crate::{computes::Computer, Args};

// 反双曲正弦函数
struct Asinh {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    Ok(Box::new(Asinh {
        field,
        target_field,
    }))
}

impl Computer for Asinh {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Float64((*mv as f64).asinh()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.asinh()),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), value),
            None => message.set(&self.field, value),
        }
    }
}
