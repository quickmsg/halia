use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ComputerConfItem;

use super::Computer;

struct Atanh {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ComputerConfItem) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Atanh {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Atanh {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => {
                    if *mv <= -1 || *mv >= 1 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64((*mv as f64).atanh())
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv <= -1.0 || *mv >= 1.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64(mv.atanh())
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
