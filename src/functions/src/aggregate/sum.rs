use message::{MessageValue, MessageBatch};

use super::Aggregater;

pub(crate) struct Sum {
    field: String,
}

impl Sum {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(Sum { field })
    }
}

impl Aggregater for Sum {
    fn aggregate(&self, mb: &MessageBatch) -> MessageValue {
        let mut sum: f64 = 0.0;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int64(value) => {
                        sum += *value as f64;
                    }
                    MessageValue::Uint64(value) => {
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

        MessageValue::Float64(sum)
    }
}
