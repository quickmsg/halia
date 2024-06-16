use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

// 反双曲余弦函数
pub(crate) struct Acosh {
    field: String,
}

impl Acosh {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Acosh { field }))
    }
}

impl Computer for Acosh {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => {
                if value >= 1.0 {
                    Some(Value::from(value.acosh()))
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
//     fn acosh() {
//         let data = r#"
//         {
//             "a": 1.0
//         }"#;
//         let message = Message::from_str(data).unwrap();
//         let mut mb = MessageBatch::from_message(message);

//         let acosh = Acosh::new("a".to_string()).unwrap();
//         acosh.operate(&mut mb);
//         let message = mb.get_one_message();
//         let a = message.get_f64("a").unwrap();
//         assert_eq!(a, 1.0_f64.acosh());

//         let data = r#"
//         {
//             "a": 2.0
//         }"#;
//         let message = Message::from_str(data).unwrap();
//         let mut mb = MessageBatch::from_message(message);
//         let acosh = Acosh::new("a".to_string()).unwrap();
//         acosh.operate(&mut mb);
//         let message = mb.get_one_message();
//         let a = message.get_f64("a").unwrap();
//         assert_eq!(a, 2.0_f64.acosh());
//     }
// }
