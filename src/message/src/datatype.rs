use std::collections::HashMap;

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