use crate::{add_or_set_message_value, computes::Computer};
use anyhow::{bail, Result};
use message::{Message, MessageValue};
use types::rules::functions::computer::StringItemConf;

struct Slice {
    field: String,
    start: usize,
    end: Option<usize>,
    target_field: Option<String>,
}

pub fn new(conf: StringItemConf) -> Result<Box<dyn Computer>> {
    let (start, end) = match conf.args {
        Some(args) => {
            if args.len() == 0 {
                bail!("Slice function requires arguments");
            }

            let start = match &args[0] {
                serde_json::Value::Number(number) => {
                    if number.is_u64() {
                        number.as_u64().unwrap() as usize
                    } else {
                        bail!("Slice function requires number arguments")
                    }
                }
                _ => bail!("Slice function requires number arguments"),
            };

            if args.len() == 2 {
                let end = match &args[1] {
                    serde_json::Value::Number(number) => {
                        if number.is_u64() {
                            Some(number.as_u64().unwrap() as usize)
                        } else {
                            bail!("Slice function requires number arguments")
                        }
                    }
                    _ => bail!("Slice function requires number arguments"),
                };
                (start, end)
            } else {
                (start, None)
            }
        }
        None => bail!("Slice function requires arguments"),
    };
    Ok(Box::new(Slice {
        field: conf.field,
        start,
        end,
        target_field: conf.target_field,
    }))
}

impl Computer for Slice {
    fn compute(&self, message: &mut Message) {
        let value = match message.get(&self.field) {
            Some(mv) => match mv {
                MessageValue::String(s) => s,
                _ => return,
            },
            None => return,
        };

        let end = match self.end {
            Some(end) => end,
            None => value.len(),
        };

        let result = match value.get(self.start..end) {
            Some(s) => MessageValue::String(s.to_string()),
            None => return,
        };

        add_or_set_message_value!(self, message, result);
    }
}
