use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

pub(crate) struct Sin {
    field: String,
}

impl Sin {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Sin { field }))
    }
}

impl Computer for Sin {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.sin())),
            None => None,
        }
    }
}
