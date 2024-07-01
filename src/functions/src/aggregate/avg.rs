use message::{MessageBatch, MessageValue};

use super::Aggregater;

pub(crate) struct Avg {
    field: String,
}

impl Avg {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(Avg { field })
    }
}

impl Aggregater for Avg {
    fn aggregate(&self, mb: &MessageBatch) -> MessageValue {
        let mut sum: f64 = 0.0;
        let mut count = 0;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int64(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    MessageValue::Uint64(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    MessageValue::Float64(value) => {
                        sum += value;
                        count += 1;
                    }
                    _ => {}
                },
                None => {}
            }
        }

        if count > 0 {
            MessageValue::Float64(sum / count as f64)
        } else {
            MessageValue::Float64(0.0)
        }
    }
}
