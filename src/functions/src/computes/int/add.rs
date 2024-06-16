use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

pub(crate) struct AddLit {
    field: String,
    value: i64,
}

impl AddLit {
    pub(crate) fn new(field: String, value: i64) -> Result<Box<dyn Computer>> {
        Ok(Box::new(AddLit { field, value }))
    }
}

impl Computer for AddLit {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_i64(&self.field) {
            Some(value) => Some(Value::from(value + self.value)),
            None => None,
        }
    }
}

pub(crate) struct AddAnother {
    field: String,
    value: String,
}

impl AddAnother {
    pub(crate) fn new(field: String, value: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(AddAnother { field, value }))
    }
}

impl Computer for AddAnother {
    fn compute(&self, message: &Message) -> Option<Value> {
        match (message.get_i64(&self.field), message.get_i64(&self.value)) {
            (Some(value1), Some(value2)) => Some(Value::from(value1 + value2)),
            _ => None,
        }
    }
}
