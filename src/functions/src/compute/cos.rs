use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ComputerConf;

use super::Computer;

struct Cos {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ComputerConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Cos {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Cos {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Float64((*mv as f64).cos()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.cos()),
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
