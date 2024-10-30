use anyhow::Result;
use message::MessageValue;
use types::rules::functions::computer::NumberItemConf;

use super::Computer;

struct Ln {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: NumberItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Ln {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Ln {
    fn compute(&self, message: &mut message::Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => {
                    if *mv <= 0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64((*mv as f64).ln())
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv <= 0.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64(mv.ln())
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
