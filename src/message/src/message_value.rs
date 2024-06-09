use std::{
    collections::HashMap,
    fmt::{self, Debug},
};

pub enum MessageValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    // Binary,
    String(String),
    Array(Vec<MessageValue>),
    Struct(HashMap<String, MessageValue>),
}

impl Debug for MessageValue {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MessageValue::Null => formatter.write_str("Null"),
            MessageValue::Boolean(boolean) => write!(formatter, "Boolean({})", boolean),
            // MessageValue::Number(number) => Debug::fmt(number, formatter),
            MessageValue::String(string) => write!(formatter, "String({:?})", string),
            // MessageValue::Array(vec) => {
            //     tri!(formatter.write_str("Array "));
            //     Debug::fmt(vec, formatter)
            // }
            // MessageValue::Object(map) => {
            //     tri!(formatter.write_str("Object "));
            //     Debug::fmt(map, formatter)
            // }
            _ => formatter.write_str("todo"),
        }
    }
}

impl MessageValue {
    pub fn get(&self, pointer: &str) -> Option<&MessageValue> {
        pointer
            .split('.')
            .skip(1)
            .map(|x| x.replace("~1", "/").replace("~0", "~"))
            .try_fold(self, |target, token| match target {
                MessageValue::Struct(map) => map.get(&token),
                // MessageValue::Array(list) => parse_index(&token).and_then(|x| list.get(x)),
                _ => None,
            })
    }
}
