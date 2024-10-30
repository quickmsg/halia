use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

use super::Aggregater;

// TODO
struct Merge {
    field: String,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Merge { field: conf.field })
}

impl Aggregater for Merge {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut values = vec![];
        let messages = mb.get_messages();
        for message in messages {
            if let Some(value) = message.get(&self.field) {
                values.push(value.clone());
            }
        }

        (self.field.clone(), MessageValue::Array(values))
    }
}