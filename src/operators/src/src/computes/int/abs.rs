use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

// 绝对值
pub(crate) struct Abs {
    field: String,
}

impl Abs {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Abs { field }))
    }
}

impl Computer for Abs {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_i64(&self.field) {
            Some(value) => {
                if value < 0 {
                    Some(Value::from(value.abs()))
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use message::Message;

    use super::*;

    #[test]
    fn abs() {
        let abs = Abs::new("a".to_string()).unwrap();

        let data = r#"
        {
            "a": -5
        }"#;
        let message = Message::from_str(data).unwrap();
        assert_eq!(abs.compute(&message), Some(Value::from(5)));

        let data = r#"
        {
            "a": 5
        }"#;
        let message = Message::from_str(data).unwrap();
        assert_eq!(abs.compute(&message), None);
    }
}
