use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

// 立方根
pub(crate) struct Cbrt {
    field: String,
}

impl Cbrt {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Cbrt { field }))
    }
}

impl Computer for Cbrt {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.cbrt())),
            None => None,
        }
    }
}
