use anyhow::Result;
use message::Message;
use sha2::{Digest, Sha224};

use crate::{add_or_set_message_value, computes::Computer, Args};

struct HaliaSha224 {
    field: String,
    target_field: Option<String>,
    hasher: Sha224,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let hasher = Sha224::new();
    Ok(Box::new(HaliaSha224 {
        field,
        target_field,
        hasher,
    }))
}

impl Computer for HaliaSha224 {
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
