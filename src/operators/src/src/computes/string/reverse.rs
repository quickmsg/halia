use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

pub(crate) struct Reverse {
    field: String,
}

impl Reverse {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Reverse { field }))
    }
}

impl Computer for Reverse {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_string(&self.field) {
            Some(value) => Some(Value::from(value.chars().rev().collect::<String>())),
            None => None,
        }
    }
}