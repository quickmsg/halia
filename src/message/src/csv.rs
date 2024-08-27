use std::io::Cursor;

use crate::{Message, MessageBatch, MessageValue};
use anyhow::{bail, Result};
use bytes::Bytes;

pub struct CsvConf {
    headers: Option<Vec<String>>,
}

impl MessageBatch {
    pub fn from_csv(bytes: Bytes) -> Result<Self> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(bytes));

        let mut mb = MessageBatch::default();

        if let Some(result) = rdr.records().next() {
            let record = result.unwrap();
            let mut message = Message::default();
            for (index, field) in record.iter().enumerate() {
                message.add(
                    format!("t{}", index),
                    MessageValue::String(field.to_owned()),
                );
            }
            mb.messages.push(message);
        }

        Ok(mb)
    }

    // TODO
    pub fn to_csv(&self) -> Vec<u8> {
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

// impl From<csv::> for Message {
//     fn from(value: serde_json::Value) -> Self {
//         Self {
//             value: MessageValue::from(value),
//         }
//     }
// }

// impl From<serde_json::Value> for MessageValue {
//     fn from(value: serde_json::Value) -> Self {
//         match value {
//             serde_json::Value::Null => Self::Null,
//             serde_json::Value::Bool(b) => Self::Boolean(b),
//             serde_json::Value::Number(n) => {
//                 match n.as_i64() {
//                     Some(v) => return Self::Int64(v),
//                     None => {}
//                 }

//                 match n.as_f64() {
//                     Some(v) => return Self::Float64(v),
//                     None => {}
//                 }

//                 Self::Null
//             }
//             serde_json::Value::String(s) => Self::String(s),
//             serde_json::Value::Array(arr) => Self::Array(arr.into_iter().map(Self::from).collect()),
//             serde_json::Value::Object(obj) => Self::Object(
//                 obj.into_iter()
//                     .map(|(key, value)| (key.into(), Self::from(value)))
//                     .collect(),
//             ),
//         }
//     }
// }

// impl Into<serde_json::Value> for MessageValue {
//     fn into(self) -> serde_json::Value {
//         match self {
//             MessageValue::Null => serde_json::Value::Null,
//             MessageValue::Boolean(b) => serde_json::Value::Bool(b),
//             MessageValue::Int64(n) => serde_json::Value::from(n),
//             MessageValue::Float64(n) => serde_json::Value::from(n),
//             MessageValue::String(s) => serde_json::Value::from(s),
//             MessageValue::Bytes(bytes) => serde_json::Value::from(bytes),
//             MessageValue::Array(arr) => {
//                 serde_json::Value::Array(arr.into_iter().map(Self::into).collect())
//             }
//             MessageValue::Object(obj) => serde_json::Value::Object(
//                 obj.into_iter()
//                     .map(|(key, value)| (key.into(), value.into()))
//                     .collect(),
//             ),
//         }
//     }
// }
