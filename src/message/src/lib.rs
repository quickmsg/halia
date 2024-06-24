use anyhow::Result;
use bytes::Bytes;
use indexmap::IndexMap;
use std::collections::HashMap;
use tracing::debug;
use value::Value;

pub mod value;

#[derive(Debug, Clone)]
pub struct MessageBatch {
    name: String,
    messages: Vec<Message>,
}

impl MessageBatch {
    // now just for tests
    pub fn from_str(s: &str) -> Result<Self> {
        let message = Message::from_str(s)?;
        Ok(MessageBatch {
            name: "_none".to_string(),
            messages: vec![message],
        })
    }

    pub fn from_json(b: &Bytes) -> Result<Self> {
        let message = Message::from_json(b)?;
        Ok(MessageBatch {
            name: "_none".to_string(),
            messages: vec![message],
        })
    }

    pub fn from_message(message: Message) -> Self {
        MessageBatch {
            name: "_none".to_string(),
            messages: vec![message],
        }
    }

    pub fn push(&mut self, message: Message) {
        self.messages.push(message);
    }

    pub fn merge(&mut self, other: Self) {
        self.messages.extend(other.messages);
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn clear(&mut self) {
        self.messages.clear();
    }

    pub fn get_messages(&self) -> &Vec<Message> {
        &self.messages
    }

    pub fn get_messages_mut(&mut self) -> &mut Vec<Message> {
        &mut self.messages
    }

    pub fn with_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    // 每个batch至少有一条数据
    pub fn get_one_message(&mut self) -> Message {
        self.messages.remove(0)
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

impl Default for MessageBatch {
    fn default() -> Self {
        Self {
            name: Default::default(),
            messages: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Message {
    value: Value,
}

impl Message {
    pub fn from_str(s: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(s)?;
        Ok(Message { value })
    }

    pub fn from_json(b: &Bytes) -> Result<Self> {
        let value: Value = json::from_slice(b)?;
        Ok(Message { value })
    }

    pub fn from_value(value: Value) -> Result<Self> {
        Ok(Message { value })
    }

    pub fn get(&self, field: &str) -> Option<&Value> {
        self.value.pointer(field)
    }

    pub fn new() -> Self {
        Message {
            value: Value::Object(HashMap::new()),
        }
    }

    // TODO fix path
    // pub fn add(&mut self, name: &str, value: Value) {
    //     match self.value.as_object_mut() {
    //         Some(object) => object.insert(name.to_string(), value),
    //         None => unreachable!(),
    //     };
    // }

    // pub fn select(&mut self, fields: &Vec<String>) {
    //     if let Some(object) = self.value.as_object_mut() {
    //         object.retain(|key, _| fields.contains(key));
    //     }
    // }

    // pub fn except(&mut self, fields: &Vec<String>) {
    //     if let Some(object) = self.value.as_object_mut() {
    //         object.retain(|key, _| !fields.contains(key));
    //     }
    // }

    // TODO
    pub fn remove(&self, fields: &Vec<String>) {
        for field in fields {}
    }

    // TODO
    pub fn rename(&self, fields: &HashMap<String, String>) {
        for (field, name) in fields {
            todo!()
        }
    }

    // pub fn get_bool(&self, name: &str) -> Option<bool> {
    //     if let Some(value) = self.get(name) {
    //         value.as_bool()
    //     } else {
    //         None
    //     }
    // }

    // pub fn set(&mut self, name: &str, set_value: Value) {
    //     let (prefix, name) = Self::get_prefix_and_name(name);
    //     match prefix {
    //         Some(prefix) => match self.get_mut(prefix) {
    //             Some(value) => match value {
    //                 Value::Array(_) => todo!(),
    //                 Value::Object(object) => {
    //                     if object.contains_key(name) {
    //                         object[name] = set_value;
    //                     } else {
    //                         object.insert(name.to_string(), set_value);
    //                     }
    //                 }
    //                 _ => {}
    //             },
    //             None => {}
    //         },
    //         None => match self.value.as_object_mut() {
    //             Some(object) => {
    //                 if object.contains_key(name) {
    //                     object[name] = set_value;
    //                 } else {
    //                     object.insert(name.to_string(), set_value);
    //                 }
    //             }
    //             None => {}
    //         },
    //     }
    // }

    // pub fn get_i64(&self, name: &str) -> Option<i64> {
    //     if let Some(value) = self.get(name) {
    //         value.as_i64()
    //     } else {
    //         None
    //     }
    // }

    // pub fn get_f64(&self, name: &str) -> Option<f64> {
    //     if let Some(value) = self.get(name) {
    //         value.as_f64()
    //     } else {
    //         None
    //     }
    // }

    // pub fn get_string(&self, name: &str) -> Option<&str> {
    //     if let Some(value) = self.get(name) {
    //         value.as_str()
    //     } else {
    //         None
    //     }
    // }

    // pub fn get_array(&self, name: &str) -> Option<&Vec<Value>> {
    //     if let Some(value) = self.get(name) {
    //         value.as_array()
    //     } else {
    //         None
    //     }
    // }

    // pub fn get_array_mut(&mut self, name: &str) -> Option<&mut Vec<Value>> {
    //     if let Some(value) = self.get_mut(name) {
    //         value.as_array_mut()
    //     } else {
    //         None
    //     }
    // }

    // pub fn as_object(&self) -> Option<&Map<String, Value>> {
    //     self.value.as_object()
    // }

    // pub fn merge(messages: &IndexMap<String, Self>) -> Message {
    //     let mut result = Message::new();
    //     for (_, message) in messages {
    //         let map = message.as_object();
    //         match map {
    //             Some(map) => {
    //                 for (key, value) in map {
    //                     // TODO fix value clone
    //                     result.add(key, value.clone());
    //                 }
    //             }
    //             None => {}
    //         }
    //     }

    //     result
    // }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            value: Value::Object(HashMap::new()),
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use serde_json::Value;

//     use crate::Message;

//     #[test]
//     fn get() {
//         let data = r#"
//         {
//             "a": 1,
//             "b": "2",
//             "c": [
//                 {
//                     "a": 1
//                 }
//             ],
//             "d": {
//                 "e": {
//                     "f": ["1", "2", "3"]
//                 }
//             }
//         }
//         "#;
//         let message = Message::from_str(data).unwrap();
//         assert_eq!(message.get("a"), Some(&Value::from(1)));
//         assert_eq!(message.get("b"), Some(&Value::from("2")));
//         assert_eq!(message.get("d.e.f.1"), Some(&Value::from("2")));
//         assert_eq!(message.get("e"), None);
//     }

//     #[test]
//     fn get_prefix_and_name() {
//         let (prefix, name) = Message::get_prefix_and_name("a.b.c.d");
//         assert_eq!(prefix, Some("a.b.c"));
//         assert_eq!(name, "d");

//         let (prefix, name) = Message::get_prefix_and_name("a");
//         assert_eq!(prefix, None);
//         assert_eq!(name, "a");
//     }
// }
