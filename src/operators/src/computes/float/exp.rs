use anyhow::Result;
use serde_json::Value;

use crate::computes::Computer;

pub struct Exp {
    field: String,
}

impl Exp {
    pub fn new(field: String) -> Result<Exp> {
        Ok(Exp { field })
    }
}

// impl Computer for Exp {
//     fn compute(&self, value: f64) -> Option<Value> {
//         match message.get_f64(&self.field) {
//             Some(value) => Some(Value::from(value)),
//             None => None,
//         }
//     }
// }
