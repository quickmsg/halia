use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;

pub struct Append {
    field: String,
    values: Vec<Value>,
}

impl Append {
    pub fn new(field: String, conf: Value) -> Result<Append> {
        let values: Vec<Value> = serde_json::from_value(conf)?;
        Ok(Append { field, values })
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
