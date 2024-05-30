use anyhow::Result;
use serde_json::Value;

use crate::computes::Computer;

pub(crate) struct Floor {
    field: String,
}

impl Floor {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Floor { field }))
    }
}

impl Computer for Floor {
    fn compute(&self, message: &message::Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.floor())),
            None => None,
        }
    }
}
