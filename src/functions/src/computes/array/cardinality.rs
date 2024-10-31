use anyhow::Result;
use message::{Message, MessageValue};
use types::rules::functions::computer::ArrayItemConf;

use crate::add_or_set_message_value;

use super::Computer;

// 数组元素数
struct Cardinality {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ArrayItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Cardinality {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Cardinality {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Array(arr) => MessageValue::Int64(arr.len() as i64),
                _ => MessageValue::Int64(-1),
            },
            None => MessageValue::Int64(-1),
        };

        add_or_set_message_value!(self, message, value);
    }
}
