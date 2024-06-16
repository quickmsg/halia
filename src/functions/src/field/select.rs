use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;
use types::graph::Operate;

pub struct Select {
    fields: Vec<String>,
}

impl Select {
    pub fn new(conf: Value) -> Result<Select> {
        let fields: Vec<String> = serde_json::from_value(conf)?;
        Ok(Select { fields })
    }
}

impl Operate for Select {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        for message in messages {
            message.select(&self.fields);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let mut mb = MessageBatch::from_str(data).unwrap();

        let conf = r#"["name"]"#;
        let conf: Value = serde_json::from_str(conf).unwrap();
        let select = Select::new(conf).unwrap();
        select.operate(&mut mb);

        let messages = mb.get_messages();
        assert_eq!(messages.len(), 1);
        let message = messages.get(0).unwrap();
        assert_ne!(message.get("/name"), None);
        assert_eq!(message.get("/age"), None);
        assert_eq!(message.get("/phones"), None);
    }
}
