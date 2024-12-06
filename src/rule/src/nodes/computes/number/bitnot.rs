use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

use crate::computes::Computer;

struct Bitnot {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Bitnot {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Bitnot {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Int64(!mv),
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
