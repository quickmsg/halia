use anyhow::Result;
use message::{Message, MessageValue};

use crate::{computes::Computer, Args};

// 反双曲余弦函数
struct Acosh {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Acosh {
        field,
        target_field,
    }))
}

impl Computer for Acosh {
    fn compute(&mut self, message: &mut Message) {
        let compute_value = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => {
                    if *mv < 1 {
                        MessageValue::Null
                    } else {
                        message::MessageValue::Float64((*mv as f64).acosh())
                    }
                }
                message::MessageValue::Float64(mv) => {
                    if *mv < 1.0 {
                        MessageValue::Null
                    } else {
                        message::MessageValue::Float64(mv.acosh())
                    }
                }
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), compute_value),
            None => message.set(&self.field, compute_value),
        }
    }
}
