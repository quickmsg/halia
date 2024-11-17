use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

// 绝对值
struct Abs {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Abs {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Abs {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Int64(mv.abs()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.abs()),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}
