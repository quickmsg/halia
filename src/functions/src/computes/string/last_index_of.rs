use anyhow::Result;
use message::{Message, MessageValue};

use crate::{computes::Computer, get_string_field_arg, Args, StringFieldArg};

const SEARCH_STRING_KEY: &str = "search_string";

struct LastIndexOf {
    field: String,
    target_field: Option<String>,
    arg: StringFieldArg,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    let arg = get_string_field_arg(&mut args, SEARCH_STRING_KEY)?;
    Ok(Box::new(LastIndexOf {
        field,
        target_field,
        arg,
    }))
}

impl Computer for LastIndexOf {
    fn compute(&mut self, message: &mut Message) {
        let value = message.get(&self.field).and_then(|mv| match mv {
            MessageValue::String(s) => Some(s),
            _ => None,
        });

        let value = match value {
            Some(s) => s,
            None => return,
        };

        let target_value = match &self.arg {
            StringFieldArg::Const(s) => s,
            StringFieldArg::Field(f) => match message.get(f) {
                Some(mv) => match mv {
                    MessageValue::String(s) => s,
                    _ => return,
                },
                None => return,
            },
        };

        let index = match value.rfind(target_value) {
            Some(p) => p as i64,
            None => -1,
        };
        let resp_value = MessageValue::Int64(index);
        match &self.target_field {
            Some(target_field) => message.add(target_field.clone(), resp_value),
            None => message.set(&self.field, resp_value),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use message::Message;
    use serde_json::json;

    use crate::{FIELD_KEY, TARGET_FIELD_KEY};

    use super::*;

    #[test]
    fn test_index_of() {
        let mut message = Message::default();
        message.add(
            "key_a".to_owned(),
            message::MessageValue::String("value_a".to_owned()),
        );

        let mut args = HashMap::new();
        args.insert(FIELD_KEY.to_owned(), json!("key_a"));
        args.insert(TARGET_FIELD_KEY.to_owned(), json!("key_b"));
        args.insert(SEARCH_STRING_KEY.to_owned(), json!("a"));
        let mut computer = super::new(args).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("key_b"), Some(&message::MessageValue::Int64(6)));

        let mut args = HashMap::new();
        args.insert(FIELD_KEY.to_owned(), json!("key_a"));
        args.insert(TARGET_FIELD_KEY.to_owned(), json!("key_b"));
        args.insert(super::SEARCH_STRING_KEY.to_owned(), json!("c"));
        let mut computer = super::new(args).unwrap();
        computer.compute(&mut message);
        assert_eq!(
            message.get("key_b"),
            Some(&message::MessageValue::Int64(-1))
        );
    }
}
