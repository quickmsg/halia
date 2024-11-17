use crate::computes::Computer;
use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ItemConf;

struct Lower {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Lower {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Lower {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => MessageValue::String(s.to_lowercase()),
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
