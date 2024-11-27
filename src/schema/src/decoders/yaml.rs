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
