use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

use super::Aggregater;

struct Count {
    field: String,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Count { field: conf.field })
}

impl Aggregater for Count {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut count = 0;
        for message in mb.get_messages() {
            if let Some(_) = message.get(&self.field) {
                count += 1;
            }
        }

        (self.field.clone(), MessageValue::Int64(count as i64))
    }
}