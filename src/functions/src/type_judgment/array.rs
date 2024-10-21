use anyhow::Result;
use async_trait::async_trait;
use message::{MessageBatch, MessageValue};
use serde_json::Value;

use crate::{add_or_set_message_value, Function};

pub struct IsArray {
    field: String,
    target_field: Option<String>,
}

impl IsArray {
    pub fn new(field: String, conf: Value) -> Result<Self> {
        todo!()
        // let values: Vec<Value> = serde_json::from_value(conf)?;
        // Ok(Append { field, values })
    }
}

#[async_trait]
impl Function for IsArray {
    async fn call(&self, message_batch: &mut MessageBatch) -> bool {
        for message in message_batch.get_messages_mut() {
            match message.get(&self.field) {
                Some(value) => match value {
                    message::MessageValue::Array(_) => {
                        add_or_set_message_value!(self, message, MessageValue::Boolean(true))
                    }
                    _ => add_or_set_message_value!(self, message, MessageValue::Boolean(false)),
                },
                None => add_or_set_message_value!(self, message, MessageValue::Boolean(false)),
            }
        }
        true
    }
}

pub struct IsNotArray {
    field: String,
    target_field: Option<String>,
}

impl IsNotArray {
    pub fn new(field: String, conf: Value) -> Result<Self> {
        todo!()
        // let values: Vec<Value> = serde_json::from_value(conf)?;
        // Ok(Append { field, values })
    }
}

#[async_trait]
impl Function for IsNotArray {
    async fn call(&self, message_batch: &mut MessageBatch) -> bool {
        for message in message_batch.get_messages_mut() {
            match message.get(&self.field) {
                Some(value) => match value {
                    message::MessageValue::Array(_) => {
                        add_or_set_message_value!(self, message, MessageValue::Boolean(false))
                    }
                    _ => add_or_set_message_value!(self, message, MessageValue::Boolean(true)),
                },
                None => add_or_set_message_value!(self, message, MessageValue::Boolean(true)),
            }
        }
        true
    }
}
