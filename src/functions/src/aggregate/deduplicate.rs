use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

use super::Aggregater;

// 去重
struct Deduplicate {
    field: String,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Deduplicate { field: conf.field })
}

impl Aggregater for Deduplicate {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut message_values = vec![];
        let mut exist_values = vec![];

        for message in mb.get_messages() {
            if let Some(value) = message.get(&self.field) {
                if exist_values.iter().find(|v| *v == value).is_none() {
                    exist_values.push(value.clone());
                    message_values.push(value.clone());
                }
            }
        }

        (self.field.clone(), MessageValue::Array(message_values))
    }
}
