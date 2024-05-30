use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

pub(crate) struct Lower {
    field: String,
}

impl Lower {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Lower { field }))
    }
}

impl Computer for Lower {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_string(&self.field) {
            Some(value) => Some(Value::from(value.to_lowercase())),
            None => None,
        }
    }
}
