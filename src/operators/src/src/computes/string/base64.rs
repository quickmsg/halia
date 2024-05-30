use crate::computes::Computer;
use anyhow::Result;
use base64::prelude::*;
use message::Message;
use serde_json::Value;

pub(crate) struct Base64 {
    field: String,
    decode: bool,
}

impl Base64 {
    pub(crate) fn new(field: String, decode: bool) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Base64 { field, decode }))
    }
}

impl Computer for Base64 {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_string(&self.field) {
            Some(value) => {
                if self.decode {
                    match BASE64_STANDARD.decode(value) {
                        Ok(value) => Some(Value::from(value)),
                        Err(_) => None,
                    }
                } else {
                    Some(Value::from(BASE64_STANDARD.encode(value)))
                }
            }
            None => None,
        }
    }
}
