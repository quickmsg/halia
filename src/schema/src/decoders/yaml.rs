use crate::Decoder;

pub struct Yaml;

impl Decoder for Yaml {
    fn decode(&self, data: bytes::Bytes) -> anyhow::Result<message::MessageBatch> {
        todo!()
    }
}
