use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

pub(crate) struct Hex {
    field: String,
    decode: bool,
}

impl Hex {
    pub(crate) fn new(field: String, decode: bool) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Hex { field, decode }))
    }
}

impl Computer for Hex {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_string(&self.field) {
            Some(value) => {
                if self.decode {
                    match hex::decode(value) {
                        Ok(value) => Some(Value::from(value)),
                        Err(_) => None,
                    }
                } else {
                    Some(Value::from(hex::encode(value)))
                }
            }
            None => None,
        }
    }
}
