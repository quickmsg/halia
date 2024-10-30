use std::{
    collections::HashMap,
    fmt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Result};

mod avro;
mod csv;
mod json;
mod ptorobuf;

#[derive(Debug, Clone)]
pub struct MessageBatch {
    ts: u64,
    name: String,
    metadata: HashMap<String, MessageValue>,
    messages: Vec<Message>,
}

#[derive(Clone)]
pub enum RuleMessageBatch {
    Owned(MessageBatch),
    Arc(Arc<MessageBatch>),
}

impl RuleMessageBatch {
    pub fn new_by_len(len: usize, mb: MessageBatch) -> RuleMessageBatch {
        if len == 1 {
            RuleMessageBatch::Owned(mb)
        } else {
            RuleMessageBatch::Arc(Arc::new(mb))
        }
    }

    pub fn take_mb(self) -> MessageBatch {
        match self {
            RuleMessageBatch::Owned(mb) => mb,
            RuleMessageBatch::Arc(mb) => (*mb).clone(),
        }
    }
}

impl MessageBatch {
    pub fn add_metadata(&mut self, key: String, value: MessageValue) {
        self.metadata.insert(key, value);
    }

    pub fn get_metadata(&self, key: &str) -> Option<&MessageValue> {
        self.metadata.get(key)
    }

    pub fn get_name(&self) -> &String {
        return &self.name;
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn get_messages(&self) -> &Vec<Message> {
        return &self.messages;
    }

    pub fn clear(&mut self) {
        self.messages.clear();
    }

    pub fn take_one_message(&mut self) -> Option<Message> {
        self.messages.pop()
    }

    pub fn get_messages_mut(&mut self) -> &mut Vec<Message> {
        return &mut self.messages;
    }

    pub fn push_message(&mut self, message: Message) {
        self.messages.push(message);
    }

    pub fn extend(&mut self, other: MessageBatch) {
        self.messages.extend(other.messages);
    }

    pub fn set_ts(&mut self, ts: u64) {
        self.ts = ts;
    }

    pub fn get_ts(&self) -> u64 {
        self.ts
    }
}

impl Default for MessageBatch {
    fn default() -> Self {
        Self {
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            name: "_none".to_string(),
            messages: Default::default(),
            metadata: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Message {
    value: MessageValue,
}

impl Message {
    pub fn get_obj(&self) -> Option<&HashMap<String, MessageValue>> {
        match &self.value {
            MessageValue::Object(map) => Some(map),
            _ => None,
        }
    }

    pub fn get(&self, field: &str) -> Option<&MessageValue> {
        self.value.get(field)
    }

    pub fn get_str(&self, field: &str) -> Option<&String> {
        match self.value.get(field) {
            Some(value) => match value {
                MessageValue::String(s) => Some(s),
                _ => None,
            },
            None => None,
        }
    }

    pub fn get_u8(&self, field: &str) -> Option<u8> {
        match self.value.get(field) {
            Some(value) => match value {
                MessageValue::Int64(n) => {
                    if *n > (u8::MAX as i64) {
                        None
                    } else {
                        Some(*n as u8)
                    }
                }
                _ => None,
            },
            None => None,
        }
    }

    pub fn get_u16(&self, field: &str) -> Option<u16> {
        match self.value.get(field) {
            Some(value) => match value {
                MessageValue::Int64(n) => {
                    if *n > (u16::MAX as i64) {
                        None
                    } else {
                        Some(*n as u16)
                    }
                }
                _ => None,
            },
            None => None,
        }
    }

    pub fn get_bool(&self, field: &str) -> Option<bool> {
        match self.value.get(field) {
            Some(value) => match value {
                MessageValue::Boolean(bool) => Some(*bool),
                _ => None,
            },
            None => None,
        }
    }

    pub fn merge(&mut self, other: Message) {
        match self.value.as_object_mut() {
            Some(s_obj) => match other.value.take_object() {
                Some(o_obj) => s_obj.extend(o_obj),
                None => {}
            },
            None => {}
        }
    }

    pub fn add(&mut self, field: String, value: MessageValue) {
        match self.value.as_object_mut() {
            Some(obj) => {
                obj.insert(field, value);
            }
            None => {}
        }
    }

    pub fn set(&mut self, field: &String, value: MessageValue) {
        match self.value.as_object_mut() {
            Some(obj) => match obj.get_mut(field) {
                Some(v) => *v = value,
                None => {}
            },
            None => {}
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            value: MessageValue::Object(HashMap::new()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum MessageValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<MessageValue>),
    Object(HashMap<String, MessageValue>),
}

impl Eq for MessageValue {}

impl MessageValue {
    pub fn from_json_number(number: serde_json::Number) -> Result<MessageValue> {
        match number.as_i64() {
            Some(v) => Ok(MessageValue::Int64(v)),
            None => match number.as_f64() {
                Some(v) => Ok(MessageValue::Float64(v)),
                None => bail!("数值过大"),
            },
        }
    }

    pub fn get(&self, pointer: &str) -> Option<&MessageValue> {
        if pointer.is_empty() {
            return Some(self);
        }
        pointer
            .split("->")
            .map(|x| x.replace("~1", "/").replace("~0", "~"))
            .try_fold(self, |target, token| match target {
                MessageValue::Object(map) => map.get(&token),
                MessageValue::Array(list) => Self::parse_index(&token).and_then(|x| list.get(x)),
                _ => None,
            })
    }

    pub fn take_object(self) -> Option<HashMap<String, MessageValue>> {
        match self {
            MessageValue::Object(obj) => Some(obj),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> Option<&mut HashMap<String, MessageValue>> {
        match self {
            MessageValue::Object(map) => Some(map),
            _ => None,
        }
    }

    fn parse_index(s: &str) -> Option<usize> {
        if s.starts_with('+') || (s.starts_with('0') && s.len() != 1) {
            return None;
        }
        s.parse().ok()
    }
}

impl Default for MessageValue {
    fn default() -> Self {
        MessageValue::Object(HashMap::new())
    }
}

impl fmt::Display for MessageValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageValue::Null => write!(f, "null"),
            MessageValue::Boolean(b) => write!(f, "{}", b),
            MessageValue::Int64(i) => write!(f, "{}", i),
            MessageValue::Float64(fl) => write!(f, "{}", fl),
            MessageValue::String(s) => write!(f, "\"{}\"", s),
            MessageValue::Bytes(bytes) => write!(f, "{:?}", bytes), // 用 `{:?}` 打印字节数组为 `Vec<u8>` 格式
            MessageValue::Array(arr) => {
                let mut result = String::from("[");
                let mut first = true;
                for value in arr {
                    if !first {
                        result.push_str(", ");
                    }
                    first = false;
                    result.push_str(&value.to_string());
                }
                result.push(']');
                write!(f, "{}", result)
            }
            MessageValue::Object(map) => {
                let mut result = String::from("{");
                let mut first = true;
                for (key, value) in map {
                    if !first {
                        result.push_str(", ");
                    }
                    first = false;
                    result.push_str(&format!("\"{}\": {}", key, value));
                }
                result.push('}');
                write!(f, "{}", result)
            }
        }
    }
}
