use message::{Message, MessageBatch};
use yaml_rust2::YamlLoader;

use crate::Decoder;

pub struct Yaml;

impl Decoder for Yaml {
    fn decode(&self, data: bytes::Bytes) -> anyhow::Result<message::MessageBatch> {
        let mut docs = YamlLoader::load_from_str(&String::from_utf8(data.to_vec())?)?;
        let mut mb = MessageBatch::default();
        while let Some(doc) = docs.pop() {
            mb.push_message(Message::from(doc));
        }

        Ok(mb)
    }
}

#[cfg(test)]
mod tests {
    use crate::Decoder;

    #[test]
    fn test_decode() {
        let test_data = r#"architecture: "replication"
auth:
  - "user"
  - 3.2
  - 5555

passwordUpdateJob:
  enabled: true"#;
        let decoder = super::Yaml;
        let mut mb = decoder.decode(bytes::Bytes::from(test_data)).unwrap();
        assert_eq!(mb.len(), 1);
        let message = mb.take_one_message().unwrap();
        assert_eq!(
            message.get("architecture"),
            Some(&message::MessageValue::String("replication".to_string()))
        );
        assert_eq!(
            message.get("passwordUpdateJob.enabled"),
            Some(&message::MessageValue::Boolean(true))
        );

        assert_eq!(
            message.get("auth.0"),
            Some(&message::MessageValue::String("user".to_string()))
        );
        assert_eq!(
            message.get("auth.1"),
            Some(&message::MessageValue::Float64(3.2))
        );
        assert_eq!(
            message.get("auth.2"),
            Some(&message::MessageValue::Int64(5555))
        );
        assert_eq!(message.get("auth.3"), None);
    }
}
