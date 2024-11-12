use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregation::ItemConf;

use crate::aggregate_return;

use super::Aggregater;

// 去重
struct Deduplicate {
    field: String,
    target_field: Option<String>,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Deduplicate {
        field: conf.field,
        target_field: conf.target_field,
    })
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

        aggregate_return!(self, MessageValue::Array(message_values))
    }
}
