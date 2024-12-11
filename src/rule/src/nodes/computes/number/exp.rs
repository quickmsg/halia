use anyhow::Result;
use message::MessageValue;

use crate::{
    add_or_set_message_value,
    nodes::{args::Args, computes::Computer},
};

pub struct Exp {
    field: String,
    target_field: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(Exp {
        field,
        target_field,
    }))
}

impl Computer for Exp {
    fn compute(&mut self, message: &mut message::Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::Int64(mv) => MessageValue::Float64((*mv as f64).exp()),
                MessageValue::Float64(mv) => MessageValue::Float64(mv.exp()),
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
    fn test_exp_int() {
        let mut message = message::Message::default();
        message.add("k".to_owned(), message::MessageValue::Int64(2));

        let mut args = std::collections::HashMap::new();
        args.insert(
            "field".to_owned(),
            serde_json::Value::String("k".to_owned()),
        );
        let mut computer = new(args.into()).unwrap();
        computer.compute(&mut message);

        assert_eq!(
            message.get("k"),
            Some(&message::MessageValue::Float64(7.38905609893065))
        );
    }
}
