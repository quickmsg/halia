use anyhow::Result;
use hmac::{Hmac, Mac};
use message::Message;
use sha1::Sha1;

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

type HmacSha1 = Hmac<Sha1>;

const KEY_KEY: &str = "key";

struct HaliaHmacSha1 {
    field: String,
    target_field: Option<String>,
    hasher: HmacSha1,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let key = args.take_string(KEY_KEY)?;
    let hasher = HmacSha1::new_from_slice(key.as_bytes())?;

    Ok(Box::new(HaliaHmacSha1 {
        field,
        target_field,
        hasher,
    }))
}

impl Computer for HaliaHmacSha1 {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(s) => s.as_bytes(),
                message::MessageValue::Bytes(vec) => vec.as_slice(),
                _ => return,
            },
            None => return,
        };

        self.hasher.update(value);
        let result = self.hasher.finalize_reset().into_bytes();
        let result = message::MessageValue::String(format!("{:x}", result));

        add_or_set_message_value!(self, message, result);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use message::Message;

    use crate::nodes::args::{Args, FIELD_KEY, TARGET_FIELD_KEY};

    use super::new;

    #[test]
    fn test_hmac_sha1() {
        let mut message = Message::default();
        message.add(
            "k".to_owned(),
            message::MessageValue::String("test_value".to_owned()),
        );

        let mut args = HashMap::new();
        args.insert(
            String::from(FIELD_KEY),
            serde_json::Value::String("k".to_owned()),
        );
        args.insert(
            TARGET_FIELD_KEY.to_owned(),
            serde_json::Value::String("k_hash".to_owned()),
        );
        args.insert(
            "key".to_owned(),
            serde_json::Value::String("test_key".to_owned()),
        );
        let args = Args::new(args);
        let mut computer = new(args).unwrap();

        computer.compute(&mut message);

        assert_eq!(
            message.get("k_hash"),
            Some(&message::MessageValue::String(
                "e1a791d33c1187176080999a04de696ddf3b2812".to_owned()
            ))
        );

        assert_eq!(
            message.get("k"),
            Some(&message::MessageValue::String("test_value".to_owned()))
        );

        // 判断computer里的hash复用是否ok
        computer.compute(&mut message);
        assert_eq!(
            message.get("k_hash"),
            Some(&message::MessageValue::String(
                "e1a791d33c1187176080999a04de696ddf3b2812".to_owned()
            ))
        );
    }
}
