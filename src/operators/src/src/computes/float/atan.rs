use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

pub struct Atan {
    field: String,
}

impl Atan {
    pub fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Atan { field }))
    }
}

impl Computer for Atan {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.atan())),
            None => None,
        }
    }
}
