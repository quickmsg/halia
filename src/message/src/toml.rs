use std::collections::HashMap;

use base64::{prelude::BASE64_STANDARD, Engine as _};
use toml::Value;

use crate::{Message, MessageValue};

impl From<Value> for Message {
    fn from(value: Value) -> Self {
        Self {
            metadatas: HashMap::new(),
            value: MessageValue::from(value),
        }
    }
}

impl From<Value> for MessageValue {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => Self::String(s),
            Value::Integer(i) => Self::Int64(i),
            Value::Float(f) => Self::Float64(f),
            Value::Boolean(b) => Self::Boolean(b),
            // datetime将会被转换为字符串
            Value::Datetime(datetime) => Self::String(datetime.to_string()),
            Value::Array(vec) => Self::Array(vec.into_iter().map(Self::from).collect()),
            Value::Table(map) => Self::Object(
                map.into_iter()
                    .map(|(key, value)| (key, Self::from(value)))
                    .collect(),
            ),
        }
    }
}

impl Into<Value> for MessageValue {
    fn into(self) -> Value {
        match self {
            MessageValue::Null => Value::String("null".to_string()),
            MessageValue::Boolean(b) => Value::Boolean(b),
            MessageValue::Int64(i) => Value::Integer(i),
            MessageValue::Float64(f) => Value::Float(f),
            MessageValue::String(s) => Value::String(s),
            MessageValue::Bytes(vec) => Value::String(BASE64_STANDARD.encode(vec)),
            MessageValue::Array(vec) => Value::Array(vec.into_iter().map(Into::into).collect()),
            MessageValue::Object(hash_map) => {
                let mut table = toml::value::Table::new();
                for (k, v) in hash_map {
                    table.insert(k, v.into());
                }
                Value::Table(table)
            }
        }
    }
}
