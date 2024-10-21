use message::{MessageBatch, MessageValue};

use crate::Function;

struct AddMetadata {
    key: String,
    value: MessageValue,
}

pub fn new_add_metadata(key: String, value: MessageValue) -> Box<dyn Function> {
    Box::new(AddMetadata { key, value })
}

#[async_trait::async_trait]
impl Function for AddMetadata {
    async fn call(&self, message_batch: &mut MessageBatch) -> bool {
        message_batch.add_metadata(self.key.clone(), self.value.clone());
        true
    }
}
