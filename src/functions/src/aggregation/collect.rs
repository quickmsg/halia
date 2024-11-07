use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

use crate::aggregate_return;

use super::Aggregater;

struct Collect {
    field: String,
    target_field: Option<String>,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Collect {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl Aggregater for Collect {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut values = vec![];
        let messages = mb.get_messages();
        for message in messages {
            if let Some(value) = message.get(&self.field) {
                values.push(value.clone());
            }
        }

        aggregate_return!(self, MessageValue::Array(values))
    }
}
