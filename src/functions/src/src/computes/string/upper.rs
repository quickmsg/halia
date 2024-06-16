use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

pub(crate) struct Upper {
    field: String,
}

impl Upper {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Upper { field }))
    }
}

impl Computer for Upper {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_string(&self.field) {
            Some(value) => Some(Value::from(value.to_uppercase())),
            None => None,
        }
    }
}