use anyhow::Result;
use message::{Message, MessageValue};

use crate::{computes::Computer, Args};

// 最小整数
struct Ceil {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Ceil {
        field,
        target_field,
    }))
}

impl Computer for Ceil {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Float64(mv) => MessageValue::Float64(mv.ceil()),
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
