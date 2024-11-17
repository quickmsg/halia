use crate::computes::Computer;
use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ItemConf;

struct Reverse {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Reverse {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Reverse {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(s) => MessageValue::Int64(s.len() as i64),
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
