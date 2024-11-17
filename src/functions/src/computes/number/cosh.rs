use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ItemConf;

use crate::computes::Computer;

struct Cosh {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Cosh {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Cosh {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Float64((*mv as f64).cosh()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.cosh()),
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
