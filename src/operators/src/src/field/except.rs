use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;
use types::graph::Operate;

pub struct Except {
    fields: Vec<String>,
}

impl Except {
    pub fn new(conf: Value) -> Result<Except> {
        let fields: Vec<String> = serde_json::from_value(conf)?;
        Ok(Except { fields })
    }
}

impl Operate for Except {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        for message in messages {
            message.except(&self.fields);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn except() {
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
        let select = Except::new(conf).unwrap();
        select.operate(&mut mb);

        let messages = mb.get_messages();
        assert_eq!(messages.len(), 1);
        let message = messages.get(0).unwrap();
        assert_eq!(message.get("/name"), None);
        assert_ne!(message.get("/age"), None);
        assert_ne!(message.get("/phones"), None);
    }
}
