use super::Aggregater;
use message::{MessageBatch, MessageValue};

struct Max {
    field: String,
}

pub const TYPE: &str = "max";

pub fn new(field: String) -> Box<dyn Aggregater> {
    Box::new(Max { field })
}

impl Aggregater for Max {
    fn aggregate(&self, mb: &MessageBatch) -> MessageValue {
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
                    MessageValue::Uint64(value) => {
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

        MessageValue::Float64(max)
    }
}
