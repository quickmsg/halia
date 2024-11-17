use anyhow::Result;
use message::Message;
use sha2::{Digest, Sha224};
use types::rules::functions::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaSha224 {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(HaliaSha224 {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for HaliaSha224 {
    fn compute(&self, message: &mut Message) {
        let resp = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(s) => {
                    let mut hasher = Sha224::new();
                    hasher.update(s);
                    let result = hasher.finalize();
                    message::MessageValue::String(format!("{:x}", result))
                }
                message::MessageValue::Bytes(vec) => {
                    let mut hasher = Sha224::new();
                    hasher.update(vec);
                    let result = hasher.finalize();
                    message::MessageValue::String(format!("{:x}", result))
                }
                _ => return,
            },
            None => return,
        };

        add_or_set_message_value!(self, message, resp);
    }
}
