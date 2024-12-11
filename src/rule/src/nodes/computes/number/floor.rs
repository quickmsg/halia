use anyhow::Result;
use message::MessageValue;

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

struct Floor {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Floor {
        field,
        target_field,
    }))
}

impl Computer for Floor {
    fn compute(&mut self, message: &mut message::Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Float64(mv) => MessageValue::Int64(mv.floor() as i64),
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
    fn test_float() {
        let mut message = message::Message::default();
        message.add("k".to_owned(), message::MessageValue::Float64(1.1));

        let mut args = std::collections::HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);

        assert_eq!(message.get("k"), Some(&message::MessageValue::Int64(1)));

        let mut message = message::Message::default();
        message.add("k".to_owned(), message::MessageValue::Float64(-1.23421));

        let mut args = std::collections::HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);

        assert_eq!(message.get("k"), Some(&message::MessageValue::Int64(-2)));
    }
}
