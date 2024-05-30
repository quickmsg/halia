// TODO move to other mod
use anyhow::Result;
use message::MessageBatch;
use serde::Deserialize;
use serde_json::Value;
use types::graph::Operate;

#[derive(Deserialize)]
pub struct Name {
    name: String,
}

impl Name {
    pub fn new(conf: Value) -> Result<Name> {
        let name: String = serde_json::from_value(conf)?;
        Ok(Name { name })
    }
}

impl Operate for Name {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        message_batch.with_name(self.name.clone());
        true
    }
}
