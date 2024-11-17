use anyhow::Result;
use md5;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaMd5 {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(HaliaMd5 {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for HaliaMd5 {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => MessageValue::String(format!("{:x}", md5::compute(s))),
                MessageValue::Bytes(b) => MessageValue::String(format!("{:x}", md5::compute(b))),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}