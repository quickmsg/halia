use anyhow::Result;
use hmac::{Hmac, Mac};
use message::Message;
use sha2::Sha384;

use crate::{
    add_or_set_message_value, computes::Computer, get_field_and_option_target_field,
    get_string_arg, Args,
};

type HmacSha384 = Hmac<Sha384>;

struct HaliaHmacSha384 {
    field: String,
    target_field: Option<String>,
    hasher: HmacSha384,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = get_field_and_option_target_field(&mut args)?;
    let key = get_string_arg(&mut args, "key")?;
    let hasher = HmacSha384::new_from_slice(key.as_bytes())?;

    Ok(Box::new(HaliaHmacSha384 {
        field,
        target_field,
        hasher,
    }))
}

impl Computer for HaliaHmacSha384 {
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

// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;

//     use message::Message;
//     use types::rules::functions::ItemConf;

//     use super::new;

//     #[test]
//     fn test_hmac_sha1() {
//         let mut message = Message::default();
//         message.add(
//             "k".to_owned(),
//             message::MessageValue::String("test_value".to_owned()),
//         );

//         let mut args = HashMap::new();
//         args.insert(
//             "key".to_owned(),
//             serde_json::Value::String("test_key".to_owned()),
//         );
//         let mut computer = new(ItemConf {
//             // TODO fix type
//             typ: types::rules::functions::Type::ArrayCardinality,
//             field: String::from("k"),
//             target_field: Some(String::from("k_hash")),
//             args: Some(args),
//         })
//         .unwrap();

//         computer.compute(&mut message);

//         assert_eq!(
//             message.get("k_hash"),
//             Some(&message::MessageValue::String(
//                 "e1a791d33c1187176080999a04de696ddf3b2812".to_owned()
//             ))
//         );

//         assert_eq!(
//             message.get("k"),
//             Some(&message::MessageValue::String("test_value".to_owned()))
//         );

//         // 判断computer里的hash复用是否ok
//         computer.compute(&mut message);
//         assert_eq!(
//             message.get("k_hash"),
//             Some(&message::MessageValue::String(
//                 "e1a791d33c1187176080999a04de696ddf3b2812".to_owned()
//             ))
//         );
//     }
// }
