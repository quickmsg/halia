use super::Aggregater;
use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

struct Min {
    field: String,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Min { field: conf.field })
}

impl Aggregater for Min {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut min = std::f64::MAX;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int64(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Float64(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    _ => {}
                },
                None => {}
            }
        }

        (self.field.clone(), MessageValue::Float64(min))
    }
}
