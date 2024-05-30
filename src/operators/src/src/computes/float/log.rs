// use anyhow::Result;
// use message::MessageBatch;
// use serde_json::Value;

// use crate::computes::Computer;

// pub struct Log {
//     field: String,
// }

// impl Log {
//     pub fn new(field: String) -> Result<Log> {
//         Ok(Log { field })
//     }
// }

// impl Computer for Log {
//     fn compute(&self, message: &message::Message) -> Option<Value> {
//         match message.get_f64(&self.field) {
//             Some(value) => Some(Value::from(value.log(2.0))),
//             None => None,
//         }
//     }
// }
