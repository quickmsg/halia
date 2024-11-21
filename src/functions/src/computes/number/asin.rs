use anyhow::Result;
use message::{Message, MessageValue};

use crate::{computes::Computer, Args};

// 反正弦函数
struct Asin {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    Ok(Box::new(Asin {
        field,
        target_field,
    }))
}

impl Computer for Asin {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(value) => match value {
                MessageValue::Int64(mv) => {
                    if *mv < 1 || *mv > 1 {
                        MessageValue::Null
                    } else {
                        message::MessageValue::Float64((*mv as f64).asin())
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv < -1.0 || *mv > 1.0 {
                        MessageValue::Null
                    } else {
                        message::MessageValue::Float64(mv.asin())
                    }
                }
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
