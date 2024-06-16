use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

pub(crate) struct Division {
    field: String,
    value: i64,
}

impl Division {
    pub(crate) fn new(field: String, value: i64) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Division { field, value }))
    }
}

impl Computer for Division {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_i64(&self.field) {
            Some(value) => Some(Value::from(value / self.value)),
            None => None,
        }
    }
}