use std::io::Cursor;

use crate::{Message, MessageBatch, MessageValue};
use anyhow::{bail, Result};
use apache_avro::{from_avro_datum, to_avro_datum, Reader, Schema};
use bytes::Bytes;

// let raw_schema_1 = r#"{
//     "name": "A",
//     "type": "record",
//     "fields": [
//         {"name": "field_one", "type": "float"}
//     ]
// }"#;

// let raw_schema_2 = r#"{
//     "name": "B",
//     "type": "record",
//     "fields": [
//         {"name": "field_one", "type": "A"}
//     ]
// }"#;

impl MessageBatch {
    pub fn from_avro(bytes: Bytes, schema: &String) -> Result<Self> {
        let reader_schema = Schema::parse_str(schema)?;
        let reader = Cursor::new(bytes);
        let reader = Reader::with_schema(&reader_schema, reader)?;
        for value in reader {
            todo!()
        }

        Ok(MessageBatch::default())
    }

    pub fn to_avro(&self) -> Vec<u8> {
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

// impl From<serde_json::Value> for Message {
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

impl TryFrom<apache_avro::types::Value> for Message {
    type Error = anyhow::Error;

    fn try_from(value: apache_avro::types::Value) -> Result<Self, Self::Error> {
        Ok(Self {
            value: MessageValue::try_from(value)?,
        })
    }
}

impl TryFrom<apache_avro::types::Value> for MessageValue {
    type Error = anyhow::Error;

    fn try_from(value: apache_avro::types::Value) -> std::result::Result<Self, Self::Error> {
        match value {
            apache_avro::types::Value::Null => Ok(Self::Null),
            apache_avro::types::Value::Boolean(b) => Ok(Self::Boolean(b)),
            apache_avro::types::Value::Int(i) => Ok(Self::Int64(i as i64)),
            apache_avro::types::Value::Long(l) => Ok(Self::Int64(l)),
            apache_avro::types::Value::Float(f) => Ok(Self::Float64(f as f64)),
            apache_avro::types::Value::Double(f) => Ok(Self::Float64(f)),
            apache_avro::types::Value::Bytes(vec) => Ok(Self::Bytes(vec)),
            apache_avro::types::Value::String(s) => Ok(Self::String(s)),
            apache_avro::types::Value::Fixed(_, vec) => todo!(),
            apache_avro::types::Value::Enum(_, _) => todo!(),
            apache_avro::types::Value::Union(_, value) => todo!(),
            apache_avro::types::Value::Array(vec) => todo!(),
            apache_avro::types::Value::Map(hash_map) => Ok(Self::Object(
                hash_map
                    .into_iter()
                    // TODO unwrap
                    .map(|(key, value)| (key.into(), Self::try_from(value).unwrap()))
                    .collect(),
            )),
            apache_avro::types::Value::Record(vec) => todo!(),
            apache_avro::types::Value::Date(_) => todo!(),
            apache_avro::types::Value::Decimal(decimal) => todo!(),
            apache_avro::types::Value::BigDecimal(big_decimal) => todo!(),
            apache_avro::types::Value::TimeMillis(_) => todo!(),
            apache_avro::types::Value::TimeMicros(_) => todo!(),
            apache_avro::types::Value::TimestampMillis(_) => todo!(),
            apache_avro::types::Value::TimestampMicros(_) => todo!(),
            apache_avro::types::Value::TimestampNanos(_) => todo!(),
            apache_avro::types::Value::LocalTimestampMillis(_) => todo!(),
            apache_avro::types::Value::LocalTimestampMicros(_) => todo!(),
            apache_avro::types::Value::LocalTimestampNanos(_) => todo!(),
            apache_avro::types::Value::Duration(duration) => todo!(),
            apache_avro::types::Value::Uuid(uuid) => todo!(),
        }
    }
}
