use anyhow::{bail, Result};
use common::get_dynamic_value_from_json;
use message::{MessageBatch, MessageValue};
use types::rules::functions::ItemConf;

struct Concat {
    field: String,
    arg: Arg,
    target_field: Option<String>,
}

enum Arg {
    Field(String),
    Value(Vec<MessageValue>),
}

impl Concat {
    pub fn new(conf: ItemConf) -> Result<Concat> {
        match conf.args {
            Some(_) => todo!(),
            None => bail!("args is required"),
        }
        let arg = match conf.args {
            Some(args) => {
                if args.len() == 1 {
                    let arg = &args[0];
                    match get_dynamic_value_from_json(arg) {
                        common::DynamicValue::Const(value) => match value {
                            serde_json::Value::Array(vec) => {
                                let message_values: Vec<MessageValue> =
                                    vec.into_iter().map(|v| MessageValue::from(v)).collect();
                                Arg::Value(message_values)
                            }
                            _ => bail!("invalid value"),
                        },
                        common::DynamicValue::Field(f) => Arg::Field(f),
                    }
                } else {
                    bail!("args length must be 1")
                }
            }
            None => bail!("args is required"),
        };

        Ok(Concat {
            field: conf.field,
            arg,
            target_field: conf.target_field,
        })
    }

    pub fn operate(&self, mb: &mut MessageBatch) {
        let messages = mb.get_messages_mut();
        for message in messages {
            // message.get_array_mut(&self.field).map(|array| {
            // TODO maybe arc
            // array.extend(self.values.clone());
            // });
        }
    }
}
