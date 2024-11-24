use anyhow::Result;
use message::MessageValue;

use crate::{
    args::{Args, StringFieldArg},
    computes::Computer,
};

const SEPARATOR_KEY: &str = "separator";

pub struct Join {
    field: String,
    target_field: Option<String>,
    separator: Option<StringFieldArg>,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let separator = args.take_option_string_field(SEPARATOR_KEY)?;
    Ok(Box::new(Join {
        field,
        target_field,
        separator,
    }))
}

impl Computer for Join {
    fn compute(&mut self, message: &mut message::Message) {
        let arr = match message.get_array(&self.field) {
            Some(arr) => arr,
            None => return,
        };

        let separator = match &self.separator {
            Some(separator) => match separator {
                StringFieldArg::Const(s) => s,
                StringFieldArg::Field(field) => match message.get_str(field) {
                    Some(v) => v,
                    None => return,
                },
            },
            None => "",
        };

        let mut result = String::new();
        for (i, mv) in arr.iter().enumerate() {
            match mv {
                MessageValue::String(s) => {
                    if i > 0 {
                        result.push_str(separator);
                    }
                    result.push_str(s);
                }
                _ => return,
            }
        }
    }
}
