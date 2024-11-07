use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

use crate::aggregate_return;

use super::Aggregater;

struct Count {
    field: String,
    target_field: Option<String>,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Count {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl Aggregater for Count {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut count = 0;
        for message in mb.get_messages() {
            if let Some(_) = message.get(&self.field) {
                count += 1;
            }
        }

        aggregate_return!(self, MessageValue::Int64(count as i64))
    }
}