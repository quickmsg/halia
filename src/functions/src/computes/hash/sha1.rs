use anyhow::Result;
use message::{Message, MessageValue};
use sha1::{Digest, Sha1};
use types::rules::functions::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaSha1 {
    field: String,
    target_field: Option<String>,
}

pub fn new(conf: ItemConf) -> Result<Box<dyn Computer>> {
    Ok(Box::new(HaliaSha1 {
        field: conf.field,
        target_field: conf.target_field,
    }))
}

impl Computer for HaliaSha1 {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => {
                    let mut hasher = Sha1::new();
                    hasher.update(s);
                    let res = hasher.finalize();
                    MessageValue::String(format!("{:x}", res))
                }
                MessageValue::Bytes(b) => {
                    let mut hasher = Sha1::new();
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

#[cfg(test)]
mod tests {
    use message::Message;
    use types::rules::functions::ItemConf;

    use super::new;

    fn test_sha1() {
        let mut message = Message::default();
        message.add(
            "k".to_owned(),
            message::MessageValue::String("test_value".to_owned()),
        );

        let mut computer = new(ItemConf {
            typ: todo!(),
            field: todo!(),
            target_field: todo!(),
            args: todo!(),
        });
    }
}
