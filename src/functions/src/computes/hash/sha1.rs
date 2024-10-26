use anyhow::Result;
use message::{Message, MessageValue};
use sha1::{Digest, Sha1 as crate_Sha1};
use types::rules::functions::computer::ItemConf;

use crate::add_or_set_message_value;

use super::Computer;

// 绝对值
struct Sha1 {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Sha1 {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for Sha1 {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => {
                    let mut hasher = crate_Sha1::new();
                    hasher.update(s);
                    let res = hasher.finalize();
                    MessageValue::String(format!("{:x}", res))
                }
                MessageValue::Bytes(b) => {
                    let mut hasher = crate_Sha1::new();
                    hasher.update(b);
                    let res = hasher.finalize();
                    MessageValue::String(format!("{:x}", res))
                }
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}
