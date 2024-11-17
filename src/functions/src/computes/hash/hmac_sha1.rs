use anyhow::Result;
use hmac::{Hmac, Mac};
use message::Message;
use sha1::Sha1;
use types::rules::functions::ItemConf;

use crate::{add_or_set_message_value, computes::Computer, get_string_arg};

type HmacSha1 = Hmac<Sha1>;

struct HaliaHmacSha1 {
    field: String,
    target_field: Option<String>,
    key: String,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    let key = get_string_arg(&conf, "key")?;
    Ok(Box::new(HaliaHmacSha1 {
        field: conf.field,
        target_field: conf.target_field,
        key,
    }))
}

impl Computer for HaliaHmacSha1 {
    fn compute(&mut self, message: &mut Message) {
        let resp = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(s) => {
                    let mut mac = HmacSha1::new_from_slice(b"my secret and secure key")
                        .expect("HMAC can take key of any size");
                    mac.update(s.as_bytes());
                    let result = mac.finalize().into_bytes();
                    message::MessageValue::String(format!("{:x}", result))
                }
                message::MessageValue::Bytes(vec) => {
                    let mut mac = HmacSha1::new_from_slice(b"my secret and secure key")
                        .expect("HMAC can take key of any size");
                    mac.update(vec);
                    let result = mac.finalize().into_bytes();
                    message::MessageValue::String(format!("{:x}", result))
                }
                _ => return,
            },
            None => return,
        };

        add_or_set_message_value!(self, message, resp);
    }
}
