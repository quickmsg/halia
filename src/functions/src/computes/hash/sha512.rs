use anyhow::Result;
use message::Message;
use sha2::{Digest, Sha512};
use types::rules::functions::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaSha512 {
    field: String,
    target_field: Option<String>,
    hasher: Sha512,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let hasher = Sha512::new();
    Ok(Box::new(HaliaSha512 {
        field: conf.field,
        target_field: conf.target_field,
        hasher,
    }))
}

impl Computer for HaliaSha512 {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(value) => match value {
                message::MessageValue::String(s) => s.as_bytes(),
                message::MessageValue::Bytes(vec) => vec.as_slice(),
                _ => return,
            },
            None => return,
        };

        self.hasher.update(value);
        let result = self.hasher.finalize_reset();
        let result = message::MessageValue::String(format!("{:x}", result));

        add_or_set_message_value!(self, message, result);
    }
}
