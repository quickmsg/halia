use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

use super::Aggregater;

struct Sum {
    field: String,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Sum { field: conf.field })
}

impl Aggregater for Sum {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut sum: f64 = 0.0;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int64(value) => {
                        sum += *value as f64;
                    }
                    MessageValue::Float64(value) => {
                        sum += value;
                    }
                    _ => {}
                },
                None => {}
            }
        }

        (self.field.clone(), MessageValue::Float64(sum))
    }
}
