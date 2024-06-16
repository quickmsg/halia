use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;

pub struct Gcd {
    field: String,
}

// TODO
impl Gcd {
    pub fn new(field: String) -> Result<Gcd> {
        Ok(Gcd { field })
    }

    // pub fn operate(&self, mb: &mut MessageBatch) {
    //     let messages = mb.get_mut_message();
    //     for message in messages {
    //         if let Some(value) = message.get_f64(&self.field) {
    //             let value = value.();
    //             message.set(&self.field, Value::from(value));
    //         }
    //     }
    // }
}
