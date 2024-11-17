use crate::{add_or_set_message_value, computes::Computer};
use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ItemConf;

struct Upper {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Upper {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Upper {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(s) => MessageValue::String(s.to_uppercase()),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}
