use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value, computes::Computer, get_array_string_field_arg, Args, StringFieldArg,
};

const STRS_KEY: &str = "strs";

struct Concat {
    field: String,
    args: Vec<StringFieldArg>,
    target_field: Option<String>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = crate::get_field_and_option_target_field(&mut args)?;
    let args = get_array_string_field_arg(&mut args, STRS_KEY)?;

    Ok(Box::new(Concat {
        field,
        target_field,
        args,
    }))
}

impl Computer for Concat {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s,
                _ => return,
            },
            None => return,
        };

        let mut result = value.clone();

        for arg in &self.args {
            match arg {
                StringFieldArg::Const(s) => result.push_str(s),
                StringFieldArg::Field(field) => match message.get(field) {
                    Some(mv) => match mv {
                        MessageValue::String(s) => result.push_str(s),
                        _ => return,
                    },
                    None => return,
                },
            }
        }

        let result = MessageValue::String(result);
        add_or_set_message_value!(self, message, result);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use message::{Message, MessageValue};
    use serde_json::json;

    use crate::{computes::string::concat::STRS_KEY, FIELD_KEY, TARGET_FIELD_KEY};

    use super::new;

    #[test]
    fn test_concat() {
        let mut message = Message::default();
        message.add(
            "key_a".to_owned(),
            message::MessageValue::String("value_a".to_owned()),
        );
        message.add(
            "key_b".to_owned(),
            message::MessageValue::String("value_b".to_owned()),
        );

        let mut args = HashMap::new();
        args.insert(
            FIELD_KEY.to_owned(),
            serde_json::Value::String("key_a".to_owned()),
        );
        args.insert(
            TARGET_FIELD_KEY.to_owned(),
            serde_json::Value::String("key_c".to_owned()),
        );
        args.insert(STRS_KEY.to_owned(), json!(["${key_b}", "value_c", "${22"]));

        let mut computer = new(args).unwrap();
        computer.compute(&mut message);

        assert_eq!(
            message.get("key_c"),
            Some(&MessageValue::String("value_avalue_bvalue_c${22".to_owned()))
        );
    }
}