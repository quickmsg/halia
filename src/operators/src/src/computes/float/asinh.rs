use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

// 反双曲正弦函数
pub(crate) struct Asinh {
    field: String,
}

impl Asinh {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Asinh { field }))
    }
}

impl Computer for Asinh {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => Some(Value::from(value.asinh())),
            None => None,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use message::Message;

//     use super::*;

//     #[test]
//     fn asinh() {
//         let data = r#"
//         {
//             "a": 1.0
//         }"#;
//         let message = Message::from_str(data).unwrap();
//         let mut mb = MessageBatch::from_message(message);

//         let asin = Asin::new("a".to_string()).unwrap();
//         asin.operate(&mut mb);
//         let message = mb.get_one_message();
//         let a = message.get_f64("a").unwrap();
//         assert_eq!(a, 1.0_f64.asin());

//         let data = r#"
//         {
//             "a": 0.5
//         }"#;
//         let message = Message::from_str(data).unwrap();
//         let mut mb = MessageBatch::from_message(message);
//         let asin = Asin::new("a".to_string()).unwrap();
//         asin.operate(&mut mb);
//         let message = mb.get_one_message();
//         let a = message.get_f64("a").unwrap();
//         assert_eq!(a, 0.5_f64.asin());
//     }
// }
