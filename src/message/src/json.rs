use crate::{Message, MessageBatch, MessageValue};
use anyhow::{bail, Result};
use bytes::Bytes;

impl MessageBatch {
    fn from_json(bytes: Bytes) -> Result<Self> {
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
}

impl From<serde_json::Value> for Message {
    fn from(value: serde_json::Value) -> Self {
        Self {
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
                if n.is_u64() {
                    Self::Uint64(n.as_u64().unwrap())
                } else if n.is_i64() {
                    Self::Int64(n.as_i64().unwrap())
                } else {
                    Self::Float64(n.as_f64().unwrap())
                }
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
