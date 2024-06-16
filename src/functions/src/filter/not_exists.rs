use message::Message;
use serde_json::Value;

use super::Filter;

pub struct NotExists {
    field: String,
}

impl NotExists {
    pub fn new(field: String) -> Box<Self> {
        Box::new(NotExists { field })
    }
}

impl Filter for NotExists {
    fn filter(&self, message: &Message) -> bool {
        match message.get(&self.field) {
            Some(value) => match value {
                Value::Null => return true,
                _ => return false,
            },
            None => return true,
        }
    }
}
