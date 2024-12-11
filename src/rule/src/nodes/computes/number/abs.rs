use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

/// 绝对值
struct Abs {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Abs {
        field,
        target_field,
    }))
}

impl Computer for Abs {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Int64(mv.abs()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.abs()),
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
    fn test_abs_int() {
        let mut message = Message::default();
        message.add("k".to_owned(), message::MessageValue::Int64(-1));

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let args = Args::new(args);

        let mut computer = new(args).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Int64(1)));

        let mut message = Message::default();
        message.add("k".to_owned(), message::MessageValue::Int64(1));

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let args = Args::new(args);

        let mut computer = new(args).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Int64(1)));
    }

    #[test]
    fn test_abs_float() {
        let mut message = Message::default();
        message.add("k".to_owned(), message::MessageValue::Float64(-1.0));

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let args = Args::new(args);

        let mut computer = new(args).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Float64(1.0)));

        let mut message = Message::default();
        message.add("k".to_owned(), message::MessageValue::Float64(1.0));

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let args = Args::new(args);

        let mut computer = new(args).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Float64(1.0)));
    }

    #[test]
    fn test_abs_other() {
        let mut message = Message::default();
        message.add(
            "k".to_owned(),
            message::MessageValue::String("test".to_owned()),
        );

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );

        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Null));

        let mut message = Message::default();
        message.add("k".to_owned(), message::MessageValue::Boolean(true));

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let args = Args::new(args);

        let mut computer = new(args).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Null));
    }
}
