use message::Message;
use serde_json::Value;

use super::Filter;

pub struct Exists {
    field: String,
}

impl Exists {
    pub fn new(field: String) -> Box<Self> {
        Box::new(Exists { field })
    }
}

impl Filter for Exists {
    fn filter(&self, message: &Message) -> bool {
        match message.get(&self.field) {
            Some(value) => match value {
                Value::Null => {
                    return false;
                }
                _ => return true,
            },
            None => return false,
        }
    }
}