use anyhow::Result;
use message::{Message, MessageValue};
use sha1::{Digest, Sha1, Sha1Core};
use sha2::digest::core_api::CoreWrapper;

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

struct HaliaSha1 {
    field: String,
    target_field: Option<String>,
    hasher: CoreWrapper<Sha1Core>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let hasher = Sha1::new();
    Ok(Box::new(HaliaSha1 {
        field,
        target_field,
        hasher,
    }))
}

impl Computer for HaliaSha1 {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(value) => match value {
                MessageValue::String(s) => s.as_bytes(),
                MessageValue::Bytes(vec) => vec.as_slice(),
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

// #[cfg(test)]
// mod tests {
//     use message::Message;
//     use types::rules::functions::ItemConf;

//     use super::new;

//     #[test]
//     fn test_sha1() {
//         let mut message = Message::default();
//         message.add(
//             "k".to_owned(),
//             message::MessageValue::String("test_value".to_owned()),
//         );

//         let mut computer = new(ItemConf {
//             typ: types::rules::functions::Type::ArrayCardinality,
//             field: String::from("k"),
//             target_field: Some(String::from("k_hash")),
//             args: None,
//         })
//         .unwrap();

//         computer.compute(&mut message);

//         assert_eq!(
//             message.get("k_hash"),
//             Some(&message::MessageValue::String(
//                 "b6dfecf3684e6a40de435195509ac724c7349c03".to_owned()
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
//                 "b6dfecf3684e6a40de435195509ac724c7349c03".to_owned()
//             ))
//         );
//     }
// }
