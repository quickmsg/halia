use anyhow::Result;
use md5 as crate_md5;
use message::{Message, MessageValue};
use types::rules::functions::computer::ItemConf;

use crate::add_or_set_message_value;

use super::Computer;

// 绝对值
struct Md5 {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Md5 {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Md5 {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => {
                    MessageValue::String(format!("{:x}", crate_md5::compute(s)))
                }
                MessageValue::Bytes(b) => {
                    MessageValue::String(format!("{:x}", crate_md5::compute(b)))
                }
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}
