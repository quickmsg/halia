use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ComputerConfItem;

use super::Computer;

// 最小整数
struct Ceil {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ComputerConfItem) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Ceil {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Ceil {
    fn compute(&self, message: &mut Message) {
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
