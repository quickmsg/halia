use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

pub(crate) struct Length {
    field: String,
}

impl Length {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Length { field }))
    }
}

impl Computer for Length {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_string(&self.field) {
            Some(value) => Some(Value::from(value.len())),
            None => None,
        }
    }
}
