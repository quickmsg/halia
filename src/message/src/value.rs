use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Uint64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
}

impl Value {
    pub fn pointer(&self, pointer: &str) -> Option<&Value> {
        if pointer.is_empty() {
            return Some(self);
        }
        pointer
            .split("->")
            .map(|x| x.replace("~1", "/").replace("~0", "~"))
            .try_fold(self, |target, token| {
                println!("{:?}, {:?}", target, token);
                match target {
                    Value::Object(map) => map.get(&token),
                    Value::Array(list) => parse_index(&token).and_then(|x| list.get(x)),
                    _ => None,
                }
            })
    }
}

fn parse_index(s: &str) -> Option<usize> {
    if s.starts_with('+') || (s.starts_with('0') && s.len() != 1) {
        return None;
    }
    s.parse().ok()
}
