use message::MessageBatch;
use yaml_rust2::YamlLoader;

use crate::Decoder;

pub struct Yaml;

impl Decoder for Yaml {
    fn decode(&self, data: bytes::Bytes) -> anyhow::Result<message::MessageBatch> {
        let mut docs = YamlLoader::load_from_str(&String::from_utf8(data.to_vec())?)?;
        if docs.len() == 0 {
            return Ok(MessageBatch::default());
        }
        // 上面已经判断过长度，所有pop不会panic
        let doc = docs.pop().unwrap();
        match doc {
            yaml_rust2::Yaml::Real(f) => {
                // let mut mb = MessageBatch::default();
                // mb.push_message(Message::from(f));
                // Ok(mb)
                // TOOD
            }
            yaml_rust2::Yaml::Integer(_) => todo!(),
            yaml_rust2::Yaml::String(_) => todo!(),
            yaml_rust2::Yaml::Boolean(_) => todo!(),
            yaml_rust2::Yaml::Array(vec) => todo!(),
            yaml_rust2::Yaml::Hash(linked_hash_map) => todo!(),
            yaml_rust2::Yaml::Alias(_) => todo!(),
            yaml_rust2::Yaml::Null => todo!(),
            yaml_rust2::Yaml::BadValue => todo!(),
        }

        todo!()
    }
}
