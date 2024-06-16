use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

pub(crate) struct Cos {
    field: String,
}

impl Cos {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Cos { field }))
    }
}

impl Computer for Cos {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.cos())),
            None => None,
        }
    }
}
