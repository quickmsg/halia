use std::collections::HashMap;
use uuid::Uuid;

mod json;

#[derive(Debug, Clone)]
pub struct MessageBatch {
    name: String,
    sink_id: Option<Uuid>,
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

    pub fn get_messages_mut(&mut self) -> &mut Vec<Message> {
        return &mut self.messages;
    }

    pub fn push_message(&mut self, message: Message) {
        self.messages.push(message);
    }

    pub fn set_sink_id(&mut self, sink_id: Uuid) {
        self.sink_id = Some(sink_id)
    }

    pub fn get_sink_id(&self) -> &Option<Uuid> {
        &self.sink_id
    }
}

impl Default for MessageBatch {
    fn default() -> Self {
        Self {
            name: "_none".to_string(),
            sink_id: None,
            messages: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Message {
    value: MessageValue,
}

impl Message {
    pub fn get(&self, field: &String) -> Option<&MessageValue> {
        return self.value.get(&field);
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
                MessageValue::Array(list) => parse_index(&token).and_then(|x| list.get(x)),
                _ => None,
            })
    }

    pub fn as_object_mut(&mut self) -> Option<&mut HashMap<String, MessageValue>> {
        match self {
            MessageValue::Object(map) => Some(map),
            _ => None,
        }
    }
}

impl Default for MessageValue {
    fn default() -> Self {
        MessageValue::Object(HashMap::new())
    }
}

fn parse_index(s: &str) -> Option<usize> {
    if s.starts_with('+') || (s.starts_with('0') && s.len() != 1) {
        return None;
    }
    s.parse().ok()
}
