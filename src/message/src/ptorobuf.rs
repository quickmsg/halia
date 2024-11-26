use crate::{Message, MessageValue};

impl From<prost_reflect::Value> for Message {
    fn from(value: prost_reflect::Value) -> Self {
        Self {
            metadatas: Default::default(),
            value: MessageValue::from(value),
        }
    }
}

impl From<prost_reflect::Value> for MessageValue {
    fn from(value: prost_reflect::Value) -> Self {
        match value {
            prost_reflect::Value::Bool(b) => Self::Boolean(b),
            prost_reflect::Value::I32(i) => Self::Int64(i as i64),
            prost_reflect::Value::I64(i) => Self::Int64(i as i64),
            prost_reflect::Value::U32(u) => Self::Int64(u as i64),
            prost_reflect::Value::U64(u) => Self::Int64(u as i64),
            prost_reflect::Value::F32(f) => Self::Float64(f as f64),
            prost_reflect::Value::F64(f) => Self::Float64(f as f64),
            prost_reflect::Value::String(s) => Self::String(s),
            prost_reflect::Value::Bytes(bytes) => Self::Bytes(bytes.to_vec()),
            prost_reflect::Value::EnumNumber(_) => todo!(),
            // prost_reflect::Value::Message(dynamic_message) => Self::Object(
            //     dynamic_message
            //         .fields()
            //         .into_iter()
            //         .map(|(field, value)| (field.into(), Self::from(value)))
            //         .collect(),
            // ),
            prost_reflect::Value::Message(_) => todo!(),
            prost_reflect::Value::List(vec) => {
                Self::Array(vec.into_iter().map(Self::from).collect())
            }
            // prost_reflect::Value::Map(hash_map) => Self::Object(
            //     hash_map
            //         .into_iter()
            //         .map(|(key, value)| (key.into(), Self::from(value)))
            //         .collect(),
            // ),
            prost_reflect::Value::Map(_) => todo!(),
        }
    }
}
