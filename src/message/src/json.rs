use std::collections::HashMap;

use crate::{Message, MessageBatch, MessageValue};
use anyhow::{bail, Result};
use bytes::Bytes;

impl MessageBatch {
    pub fn from_json(bytes: Bytes) -> Result<Self> {
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        match value {
            serde_json::Value::Array(values) => {
                let mut mb = MessageBatch::default();
                for value in values {
                    mb.messages.push(Message::from(value));
                }
                Ok(mb)
            }
            serde_json::Value::Object(_) => {
                let mut mb = MessageBatch::default();
                mb.messages.push(Message::from(value));
                Ok(mb)
            }
            _ => bail!("not support type"),
        }
    }

    pub fn to_json(&self) -> Vec<u8> {
        let messages = self.get_messages();
        if messages.len() == 0 {
            vec![]
        } else if messages.len() == 1 {
            let value: serde_json::Value = messages[0].value.clone().into();
            serde_json::to_vec(&value).unwrap()
        } else {
            let mut values: Vec<serde_json::Value> = vec![];
            for message in messages {
                values.push(message.value.clone().into())
            }
            serde_json::to_vec(&values).unwrap()
        }
    }
}

impl From<serde_json::Value> for Message {
    fn from(value: serde_json::Value) -> Self {
        Self {
            metadata: HashMap::new(),
            value: MessageValue::from(value),
        }
    }
}

impl From<serde_json::Value> for MessageValue {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(b) => Self::Boolean(b),
            serde_json::Value::Number(n) => {
                match n.as_i64() {
                    Some(v) => return Self::Int64(v),
                    None => {}
                }

                match n.as_f64() {
                    Some(v) => return Self::Float64(v),
                    None => {}
                }

                Self::Null
            }
            serde_json::Value::String(s) => Self::String(s),
            serde_json::Value::Array(arr) => Self::Array(arr.into_iter().map(Self::from).collect()),
            serde_json::Value::Object(obj) => Self::Object(
                obj.into_iter()
                    .map(|(key, value)| (key.into(), Self::from(value)))
                    .collect(),
            ),
        }
    }
}

impl Into<serde_json::Value> for MessageValue {
    fn into(self) -> serde_json::Value {
        match self {
            MessageValue::Null => serde_json::Value::Null,
            MessageValue::Boolean(b) => serde_json::Value::Bool(b),
            MessageValue::Int64(n) => serde_json::Value::from(n),
            MessageValue::Float64(n) => serde_json::Value::from(n),
            MessageValue::String(s) => serde_json::Value::from(s),
            MessageValue::Bytes(bytes) => serde_json::Value::from(bytes),
            MessageValue::Array(arr) => {
                serde_json::Value::Array(arr.into_iter().map(Self::into).collect())
            }
            MessageValue::Object(obj) => serde_json::Value::Object(
                obj.into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            ),
        }
    }
}
