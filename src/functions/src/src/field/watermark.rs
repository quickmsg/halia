use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use types::graph::Operate;

pub struct Watermark {}

impl Watermark {
    pub fn new(conf: Value) -> Result<Watermark> {
        // let fields: Vec<String> = serde_json::from_value(conf)?;
        Ok(Watermark {})
    }
}

impl Operate for Watermark {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        let mut ts = 0;
        let time = SystemTime::now().duration_since(UNIX_EPOCH);
        match time {
            Ok(duration) => {
                ts = duration.as_secs();
            }
            Err(_) => {}
        }

        let messages = message_batch.get_messages_mut();
        for message in messages {
            message.add("_ts", ts.into());
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark() {
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

        let watermark = Watermark::new(Value::default()).unwrap();
        watermark.operate(&mut mb);

        let messages = mb.get_messages();
        let message = messages.get(0).unwrap();
        assert_ne!(message.get("/_ts"), None);
    }
}
