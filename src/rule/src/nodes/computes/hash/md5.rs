use anyhow::Result;
use md5::Digest;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

struct HaliaMd5 {
    field: String,
    target_field: Option<String>,
    hasher: md5::Md5,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let hasher = md5::Md5::new();
    Ok(Box::new(HaliaMd5 {
        field,
        target_field,
        hasher,
    }))
}

impl Computer for HaliaMd5 {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s.as_bytes(),
                MessageValue::Bytes(b) => b.as_slice(),
                _ => return,
            },
            None => return,
        };

        self.hasher.update(value);
        let result = self.hasher.finalize_reset();

        let result = MessageValue::String(format!("{:x}", result));

        add_or_set_message_value!(self, message, result);
    }
}
