use anyhow::Result;
use message::{Message, MessageValue};

use crate::{add_or_set_message_value, computes::Computer, Args};

static TARGET_LEGNTH_KEY: &str = "target_length";
static PAD_STRING_KEY: &str = "pad_string";

struct PadStart {
    field: String,
    target_field: Option<String>,
    target_length: usize,
    pad_string: Option<String>,
}

pub fn validate_conf(mut args: Args) -> Result<()> {
    args.validate_field_and_option_target_field()?;
    // TODO
    Ok(())
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    let target_length = args.take_usize(TARGET_LEGNTH_KEY)?;
    let pad_string = args.take_option_string(PAD_STRING_KEY)?;

    Ok(Box::new(PadStart {
        field,
        target_field,
        target_length,
        pad_string,
    }))
}

impl Computer for PadStart {
    fn compute(&mut self, message: &mut Message) {
        let value = match message.get_str(&self.field) {
            Some(mv) => mv,
            None => return,
        };

        let result = {
            if value.len() >= self.target_length {
                value.to_string()
            } else {
                let pad_len = self.target_length - value.len();
                let repeated = {
                    match &self.pad_string {
                        Some(pad_string) => pad_string.repeat(
                            (pad_len + self.pad_string.as_ref().unwrap().len() - 1)
                                / self.pad_string.as_ref().unwrap().len(),
                        ),
                        None => " ".repeat(pad_len),
                    }
                };
                let padded_part = &repeated[..pad_len];
                format!("{}{}", padded_part, value)
            }
        };

        add_or_set_message_value!(self, message, MessageValue::String(result));
    }
}
