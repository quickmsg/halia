use anyhow::Result;
use message::MessageValue;

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

struct Ln {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Ln {
        field,
        target_field,
    }))
}

impl Computer for Ln {
    fn compute(&mut self, message: &mut message::Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => {
                    if *mv <= 0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64((*mv as f64).ln())
                    }
                }
                MessageValue::Float64(mv) => {
                    if *mv <= 0.0 {
                        MessageValue::Null
                    } else {
                        MessageValue::Float64(mv.ln())
                    }
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
    use super::*;

    #[test]
    fn test_ln_int() {
        let mut message = message::Message::default();
        message.add("k".to_owned(), message::MessageValue::Int64(10));

        let mut args = std::collections::HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);

        assert_eq!(
            message.get("k"),
            Some(&message::MessageValue::Float64(2.302585092994046))
        );
    }

    #[test]
    fn test_ln_float() {
        let mut message = message::Message::default();
        message.add("k".to_owned(), message::MessageValue::Float64(2.0));

        let mut args = std::collections::HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);

        assert_eq!(
            message.get("k"),
            Some(&message::MessageValue::Float64(2.0_f64.ln()))
        );
    }

    #[test]
    fn test_ln_int_zero() {
        let mut message = message::Message::default();
        message.add("k".to_owned(), message::MessageValue::Int64(0));

        let mut args = std::collections::HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);

        assert_eq!(message.get("k"), Some(&message::MessageValue::Null));
    }
}
