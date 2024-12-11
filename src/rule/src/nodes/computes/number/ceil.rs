use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

// 最小整数
struct Ceil {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Ceil {
        field,
        target_field,
    }))
}

impl Computer for Ceil {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Int64(*mv),
                MessageValue::Float64(mv) => MessageValue::Int64(mv.ceil() as i64),
                _ => MessageValue::Null,
            },
            None => MessageValue::Null,
        };

        add_or_set_message_value!(self, message, value);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_int() {
        let mut message = Message::default();
        message.add("k".to_owned(), message::MessageValue::Int64(2));

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Int64(2)));
    }

    #[test]
    fn test_float() {
        let mut message = Message::default();
        message.add("k".to_owned(), message::MessageValue::Float64(1.23123));

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Int64(2)));

        let mut message = Message::default();
        message.add("k".to_owned(), message::MessageValue::Float64(-1.23123));

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Int64(-1)));
    }
}
