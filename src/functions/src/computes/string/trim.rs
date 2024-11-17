use crate::computes::Computer;
use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ItemConf;

struct Trim {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Trim {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Trim {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => MessageValue::String(s.trim().to_string()),
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
