use std::collections::HashMap;

use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, get_field_and_option_target_field};

use super::Computer;

// 数组元素数
struct Cardinality {
    field: String,
    target_field: Option<String>,
}

pub fn new(mut args: HashMap<String, serde_json::Value>) -> Result<Box<dyn Computer>> {
    let (field, target_field) = get_field_and_option_target_field(&mut args)?;
    Ok(Box::new(Cardinality {
        field,
        target_field,
    }))
}

impl Computer for Cardinality {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Array(arr) => MessageValue::Int64(arr.len() as i64),
                _ => return,
            },
            None => return,
        };

        add_or_set_message_value!(self, message, value);
    }
}
