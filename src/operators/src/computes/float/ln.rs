use anyhow::Result;
use serde_json::Value;

use crate::computes::Computer;

pub(crate) struct Ln {
    field: String,
}

impl Ln {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Ln { field }))
    }
}

impl Computer for Ln {
    fn compute(&self, message: &message::Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.ln())),
            None => None,
        }
    }
}
