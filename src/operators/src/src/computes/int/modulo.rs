use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

pub(crate) struct Modulo {
    field: String,
    value: i64,
}

impl Modulo {
    pub(crate) fn new(field: String, value: i64) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Modulo { field, value }))
    }
}

impl Computer for Modulo {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_i64(&self.field) {
            Some(value) => Some(Value::from(value % self.value)),
            None => None,
        }
    }
}