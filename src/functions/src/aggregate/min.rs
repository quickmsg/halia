use super::Aggregater;
use message::{MessageBatch, MessageValue};

struct Min {
    field: String,
}

pub const TYPE: &str = "min";

    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(Min { field })
    }

impl Aggregater for Min {
    fn aggregate(&self, mb: &MessageBatch) -> MessageValue {
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
                    MessageValue::Uint64(value) => {
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

        MessageValue::Float64(min)
    }
}
