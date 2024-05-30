// use anyhow::Result;
// use serde_json::Value;

// use crate::computes::Computer;

// pub struct Power {
//     field: String,
// }

// impl Power {
//     pub fn new(field: String) -> Result<Power> {
//         Ok(Power { field })
//     }
// }

// impl Computer for Power {
//     fn compute(&self, message: &message::Message) -> Option<Value> {
//         match message.get_f64(&self.field) {
//             // TODO
//             Some(value) => Some(Value::from(value.powf(2.0))),
//             None => None,
//         }
//     }
// }
