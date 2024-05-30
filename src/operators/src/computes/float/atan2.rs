use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

pub struct Atan2 {
    field: String,
    other: f64,
}

impl Atan2 {
    pub fn new(field: String, other: f64) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Atan2 { field, other }))
    }
}

impl Computer for Atan2 {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.atan2(self.other))),
            None => None,
        }
    }
}
