use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;

use super::Operator;

#[derive(Deserialize)]
struct Move {
    origin_field: String,
    target_field: String,
}

pub fn new(conf: Value) -> Result<Box<dyn Operator>> {
    let mov: Move = serde_json::from_value(conf)?;
    Ok(Box::new(mov))
}

impl Operator for Move {
    fn operate(&self, mb: &mut message::MessageBatch) {
        for message in mb.get_messages_mut().iter_mut() {
            // message
        }
    }
}
