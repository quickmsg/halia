use async_trait::async_trait;
use message::{MessageBatch, MessageValue};

use crate::{add_or_set_message_value, Function};

pub struct ToInt {
    field: String,
    target_field: Option<String>,
}

pub fn new(field: String, target_field: Option<String>) -> Box<dyn Function> {
    Box::new(ToInt {
        field,
        target_field,
    })
}

#[async_trait]
impl Function for ToInt {
    async fn call(&mut self, message_batch: &mut MessageBatch) -> bool {
        for message in message_batch.get_messages_mut() {
            match message.get(&self.field) {
                Some(value) => match value {
                    message::MessageValue::Float64(f) => {
                        add_or_set_message_value!(self, message, MessageValue::Int64(*f as i64))
                    }
                    MessageValue::Boolean(b) => match *b {
                        true => add_or_set_message_value!(self, message, MessageValue::Int64(1)),
                        false => add_or_set_message_value!(self, message, MessageValue::Int64(0)),
                    },
                    MessageValue::Int64(_) => {}
                    MessageValue::String(s) => match s.parse::<i64>() {
                        Ok(v) => add_or_set_message_value!(self, message, MessageValue::Int64(v)),
                        Err(_) => add_or_set_message_value!(self, message, MessageValue::Null),
                    },
                    MessageValue::Bytes(vec) => todo!(),
                    _ => add_or_set_message_value!(self, message, MessageValue::Null),
                },
                None => add_or_set_message_value!(self, message, MessageValue::Null),
            }
        }
        true
    }
}
