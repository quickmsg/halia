use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

pub(crate) struct Atanh {
    field: String,
}

impl Atanh {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Atanh { field }))
    }
}

impl Computer for Atanh {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.atanh())),
            None => None,
        }
    }
}
