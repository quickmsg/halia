use anyhow::Result;
use message::Message;
use serde_json::Value;

use crate::computes::Computer;

// 反余弦函数
pub struct Acos {
    field: String,
}

impl Acos {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Acos { field }))
    }
}

impl Computer for Acos {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_f64(&self.field) {
            Some(value) => {
                if value >= -1.0 && value <= 1.0 {
                    Some(Value::from(value.acos()))
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
    fn acos() {
        let acos = Acos::new("a".to_string()).unwrap();

        let data = r#"
        {
            "a": 0.3
        }"#;
        let message = Message::from_str(data).unwrap();
        assert_eq!(acos.compute(&message), Some(Value::from(0.3_f64.acos())));

        let data = r#"
        {
            "a": 2.0
        }"#;
        let message = Message::from_str(data).unwrap();
        assert_eq!(acos.compute(&message), None);
    }
}
