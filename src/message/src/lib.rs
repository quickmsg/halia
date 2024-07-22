use std::collections::HashMap;

mod json;

#[derive(Debug, Clone)]
pub struct MessageBatch {
    name: String,
    messages: Vec<Message>,
}

impl MessageBatch {
    pub fn get_name(&self) -> &String {
        return &self.name;
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn get_messages(&self) -> &Vec<Message> {
        return &self.messages;
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
}

impl Default for MessageBatch {
    fn default() -> Self {
        Self {
            name: "_none".to_string(),
            messages: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Message {
    value: MessageValue,
}

impl Message {
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
                MessageValue::Uint64(n) => {
                    if *n > (u8::MAX as u64) {
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
                MessageValue::Uint64(n) => {
                    if *n > (u16::MAX as u64) {
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
        self.value.as_object_mut().unwrap().insert(field, value);
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            value: MessageValue::Object(HashMap::new()),
        }
    }
}

#[derive(Clone, Debug)]
pub enum MessageValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Uint64(u64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<MessageValue>),
    Object(HashMap<String, MessageValue>),
}

impl MessageValue {
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
