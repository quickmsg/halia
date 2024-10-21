use anyhow::Result;
use async_trait::async_trait;
use message::{MessageBatch, MessageValue};
use serde_json::Value;

use crate::{add_or_set_message_value, Function};

pub struct ToString {
    field: String,
    target_field: Option<String>,
    precision: usize,
}

impl ToString {
    pub fn new(field: String, conf: Value) -> Result<Self> {
        todo!()
        // let values: Vec<Value> = serde_json::from_value(conf)?;
        // Ok(Append { field, values })
    }
}

#[async_trait]
impl Function for ToString {
    async fn call(&self, message_batch: &mut MessageBatch) -> bool {
        for message in message_batch.get_messages_mut() {
            match message.get(&self.field) {
                Some(value) => match value {
                    message::MessageValue::Float64(f) => {
                        add_or_set_message_value!(
                            self,
                            message,
                            MessageValue::String(format!(
                                "{:.precision$}",
                                f,
                                precision = self.precision
                            ))
                        )
                    }
                    MessageValue::Null => {
                        add_or_set_message_value!(
                            self,
                            message,
                            MessageValue::String("null".to_owned())
                        )
                    }
                    MessageValue::Boolean(b) => match *b {
                        true => add_or_set_message_value!(
                            self,
                            message,
                            MessageValue::String("true".to_owned())
                        ),
                        false => add_or_set_message_value!(
                            self,
                            message,
                            MessageValue::String("false".to_owned())
                        ),
                    },
                    MessageValue::Int64(i) => {
                        add_or_set_message_value!(
                            self,
                            message,
                            MessageValue::String(i.to_string())
                        )
                    }
                    MessageValue::String(_) => {}
                    MessageValue::Bytes(vec) => todo!(),
                    MessageValue::Array(vec) =>
                    // self,
                    // message,
                    // MessageValue::String(serde_json::to_string(vec).unwrap())
                    {
                        todo!()
                    }
                    MessageValue::Object(hash_map) => todo!(),
                    // _ => add_or_set_message_value!(self, message, MessageValue::Null),
                },
                None => add_or_set_message_value!(self, message, MessageValue::Null),
            }
        }
        true
    }
}
