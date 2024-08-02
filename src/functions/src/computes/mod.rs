use anyhow::{bail, Result};
use message::{Message, MessageBatch};
use serde::Deserialize;
use serde_json::Value;
use tracing::debug;

use crate::Function;

// mod float;
// mod int;
// mod string;

// pub(crate) trait Computer: Sync + Send {
//     fn compute(&self, message: &mut Message);
// }

#[derive(Deserialize)]
pub(crate) struct Rule {
    r#type: String,
    name: String,
    field: String,
    output_field: String,
    value: Option<Value>,
    values: Option<Vec<Value>>,
}

struct ComputeInfo {
    output_field: String,
    // computer: Box<dyn Computer>,
}

pub struct ComputeNode {
    compute_infos: Vec<ComputeInfo>,
}

impl ComputeNode {
    pub fn new(conf: Value) -> Result<Box<dyn Function>> {
        let mut compute_infos: Vec<ComputeInfo> = Vec::new();
        let rules: Vec<Rule> = serde_json::from_value(conf)?;
        for rule in &rules {
            match rule.r#type.as_str() {
                // "float" => match get_float_computer(rule) {
                //     Ok(computer) => compute_infos.push(ComputeInfo {
                //         output_field: rule.output_field.clone(),
                //         computer,
                //     }),
                //     Err(e) => bail!("{}", e),
                // },
                // "int" => match get_int_computer(rule) {
                //     Ok(computer) => compute_infos.push(ComputeInfo {
                //         output_field: rule.output_field.clone(),
                //         computer,
                //     }),
                //     Err(e) => bail!("{}", e),
                // },
                _ => {}
            }
        }

        Ok(Box::new(ComputeNode { compute_infos }))
    }
}

impl Function for ComputeNode {
    fn call(&self, message_batch: &mut MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        for message in messages {
            for ci in &self.compute_infos {
                // ci.computer.compute(message);
                // Some(value) => {
                //     debug!("{}, {}", ci.output_field, value);
                //     // message.set(&ci.output_field, value);
                // }
                // None => {}
            }
        }
        true
    }
}
