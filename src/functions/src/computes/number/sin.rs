use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::computer::NumberItemConf;

use super::Computer;

struct Sin {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: NumberItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Sin {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Sin {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Float64((*mv as f64).sin()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.sin()),
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
