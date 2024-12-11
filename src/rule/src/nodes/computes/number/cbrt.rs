use anyhow::Result;
use message::{Message, MessageValue};

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

// 立方根
struct Cbrt {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Cbrt {
        field,
        target_field,
    }))
}

impl Computer for Cbrt {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Float64((*mv as f64).cbrt()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.cbrt()),
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

    #[test]
    fn test_cbrt_int() {
        use super::*;

        let mut message = Message::default();
        message.add("k".to_owned(), message::MessageValue::Int64(27));

        let mut args = HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);
        assert_eq!(message.get("k"), Some(&message::MessageValue::Float64(3.0)));
    }
}
