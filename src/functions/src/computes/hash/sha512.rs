use anyhow::Result;
use message::Message;
use sha2::{Digest, Sha512};
use types::rules::functions::computer::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaSha512 {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(HaliaSha512 {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for HaliaSha512 {
    fn compute(&self, message: &mut Message) {
        let resp = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(s) => {
                    let mut hasher = Sha512::new();
                    hasher.update(s);
                    let result = hasher.finalize();
                    message::MessageValue::String(format!("{:x}", result))
                }
                message::MessageValue::Bytes(vec) => {
                    let mut hasher = Sha512::new();
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
