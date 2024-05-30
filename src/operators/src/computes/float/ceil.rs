use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

// 最小整数
pub(crate) struct Ceil {
    field: String,
}

impl Ceil {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Ceil { field }))
    }
}

impl Computer for Ceil {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.ceil())),
            None => None,
        }
    }
}
