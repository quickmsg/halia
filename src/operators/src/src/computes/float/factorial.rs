use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;

pub struct Factorial {
    field: String,
}

// TODO not find correct function
// impl Factorial {
//     pub fn new(field: String) -> Result<Factorial> {
//         Ok(Factorial { field })
//     }

//     pub fn operate(&self, mb: &mut MessageBatch) {
//         let messages = mb.get_mut_message();
//         for message in messages {
//             if let Some(value) = message.get_f64(&self.field) {
//                 let value = value.fract();
//                 message.set(&self.field, Value::from(value));
//             }
//         }
//     }
// }
