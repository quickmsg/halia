use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

// 反正弦函数
pub(crate) struct Asin {
    field: String,
}

impl Asin {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Asin { field }))
    }
}

impl Computer for Asin {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => {
                if value >= -1.0 && value <= 1.0 {
                    Some(Value::from(value.asin()))
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use message::Message;

//     use super::*;

//     #[test]
//     fn asin() {
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
