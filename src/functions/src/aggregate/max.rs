use super::Aggregater;
use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

struct Max {
    field: String,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Max { field: conf.field })
}

impl Aggregater for Max {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut max = std::f64::MIN;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int64(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    MessageValue::Float64(value) => {
                        if *value > max {
                            max = *value
                        }
                    }
                    _ => {}
                },
                None => {}
            }
        }

        (self.field.clone(), MessageValue::Float64(max))
    }
}
