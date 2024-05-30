use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

pub(crate) struct Cosh {
    field: String,
}

impl Cosh {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Cosh { field }))
    }
}

impl Computer for Cosh {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.cosh())),
            None => None,
        }
    }
}
