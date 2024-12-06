use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value,
    nodes::{
        args::{Args, StringFieldArg},
        computes::Computer,
    },
};

const SEARCH_STRING_KEY: &str = "search_string";

struct IndexOf {
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
    let search_string = args.take_string_field(SEARCH_STRING_KEY)?;
    Ok(Box::new(IndexOf {
        field,
        target_field,
        search_string,
    }))
}

impl Computer for IndexOf {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(s) => s,
            None => return,
        };

        let target_value = match &self.search_string {
            StringFieldArg::Const(s) => s,
            StringFieldArg::Field(f) => match message.get_str(f) {
                Some(mv) => mv,
                None => return,
            },
        };

        let index = match value.find(target_value) {
            Some(p) => p as i64,
            None => -1,
        };

        add_or_set_message_value!(self, message, MessageValue::Int64(index));
    }
}

// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;

//     use message::Message;
//     use serde_json::json;

//     use crate::args::{FIELD_KEY, TARGET_FIELD_KEY};

//     use super::*;

//     #[test]
//     fn test_index_of() {
//         let mut message = Message::default();
//         message.add(
//             "key_a".to_owned(),
//             message::MessageValue::String("value_a".to_owned()),
//         );

//         let mut args = HashMap::new();
//         args.insert(FIELD_KEY.to_owned(), json!("key_a"));
//         args.insert(TARGET_FIELD_KEY.to_owned(), json!("key_b"));
//         args.insert(SEARCH_STRING_KEY.to_owned(), json!("a"));
//         let args = Args::new(args);
//         let mut computer = super::new(args).unwrap();
//         computer.compute(&mut message);
//         assert_eq!(message.get("key_b"), Some(&message::MessageValue::Int64(1)));

//         let mut args = HashMap::new();
//         args.insert(FIELD_KEY.to_owned(), json!("key_a"));
//         args.insert(TARGET_FIELD_KEY.to_owned(), json!("key_b"));
//         args.insert(super::SEARCH_STRING_KEY.to_owned(), json!("c"));
//         let args = Args::new(args);
//         let mut computer = super::new(args).unwrap();
//         computer.compute(&mut message);
//         assert_eq!(
//             message.get("key_b"),
//             Some(&message::MessageValue::Int64(-1))
//         );
//     }
// }
