use crate::computes::Computer;
use anyhow::Result;
use message::{Message, MessageValue};

// 绝对值
struct Abs {
    field: String,
    target_field: String,
}

pub fn new(field: String, target_field: String) -> Result<Box<dyn Computer>> {
    Ok(Box::new(Abs {
        field,
        target_field,
    }))
}

impl Computer for Abs {
    fn compute(&self, message: &mut Message) {
        match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::Int64(mv) => {
                    message.add(self.target_field.clone(), MessageValue::Int64(mv.abs()));
                }
                message::MessageValue::Float64(mv) => {
                    message.add(self.target_field.clone(), MessageValue::Float64(mv.abs()));
                }
                _ => {}
            },
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use message::Message;

    use super::*;

    #[test]
    fn abs() {
        // let abs = Abs::new("a".to_string()).unwrap();

        // let data = r#"
        // {
        //     "a": -5
        // }"#;
        // let message = Message::from_str(data).unwrap();
        // assert_eq!(abs.compute(&message), Some(Value::from(5)));

        // let data = r#"
        // {
        //     "a": 5
        // }"#;
        // let message = Message::from_str(data).unwrap();
        // assert_eq!(abs.compute(&message), None);
    }
}
