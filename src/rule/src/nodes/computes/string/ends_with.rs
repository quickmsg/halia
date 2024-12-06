use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value,
    nodes::{
        args::{Args, StringFieldArg},
        computes::Computer,
    },
};

struct EndsWith {
    field: String,
    target_field: Option<String>,
    search_string: StringFieldArg,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let search_string = args.take_string_field("search_string")?;

    Ok(Box::new(EndsWith {
        field,
        target_field,
        search_string,
    }))
}

impl Computer for EndsWith {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(s) => s,
            None => return,
        };

        let search_string = match &self.search_string {
            StringFieldArg::Const(s) => s,
            StringFieldArg::Field(f) => match message.get_str(f) {
                Some(v) => v,
                None => return,
            },
        };

        let result = MessageValue::Boolean(value.ends_with(search_string));
        add_or_set_message_value!(self, message, result);
    }
}

// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;

//     use message::Message;
//     use serde_json::json;

//     use crate::args::{FIELD_KEY, TARGET_FIELD_KEY};

//     use super::SEARCH_STRING_KEY;

//     #[test]
//     fn test_ends_with() {
//         let mut message = Message::default();
//         message.add(
//             "key_a".to_owned(),
//             message::MessageValue::String("value_a".to_owned()),
//         );

//         let mut args = HashMap::new();
//         args.insert(FIELD_KEY.to_owned(), json!("key_a"));
//         args.insert(TARGET_FIELD_KEY.to_owned(), json!("key_b"));
//         args.insert(SEARCH_STRING_KEY.to_owned(), json!("a"));
//         let args = super::Args::new(args);
//         let mut computer = super::new(args).unwrap();
//         computer.compute(&mut message);
//         assert_eq!(
//             message.get("key_b"),
//             Some(&message::MessageValue::Boolean(true))
//         );

//         let mut args = HashMap::new();
//         args.insert(FIELD_KEY.to_owned(), json!("key_a"));
//         args.insert(TARGET_FIELD_KEY.to_owned(), json!("key_b"));
//         args.insert(SEARCH_STRING_KEY.to_owned(), json!("b"));
//         let args = super::Args::new(args);
//         let mut computer = super::new(args).unwrap();
//         computer.compute(&mut message);
//         assert_eq!(
//             message.get("key_b"),
//             Some(&message::MessageValue::Boolean(false))
//         );
//     }
// }
