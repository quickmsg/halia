use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, computes::Computer, Args};

// 反余弦函数
struct Acos {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Acos {
        field,
        target_field,
    }))
}

impl Computer for Acos {
    fn compute(&mut self, message: &mut Message) {
        let result = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => {
                    if *mv < -1 || *mv > 1 {
                        return;
                    } else {
                        MessageValue::Float64((*mv as f64).acos())
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv < -1.0 || *mv > 1.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64(mv.acos())
                    }
                }
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, result);
    }
}
