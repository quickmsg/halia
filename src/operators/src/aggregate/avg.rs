use message::MessageBatch;
use serde_json::Value;

use super::Aggregater;

pub(crate) struct AvgInt {
    field: String,
}

impl AvgInt {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(AvgInt { field })
    }
}

impl Aggregater for AvgInt {
    fn aggregate(&self, mb: &MessageBatch) -> Value {
        let mut sum = 0;
        let mut count = 0;
        let messages = mb.get_messages();
        for message in messages {
            match message.get_i64(&self.field) {
                Some(value) => {
                    count += 1;
                    sum += value;
                }
                None => {}
            }
        }

        let avg = sum / count;

        Value::from(avg)
    }
}

pub struct AvgFloat {
    field: String,
}

impl AvgFloat {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(AvgFloat { field })
    }
}

impl Aggregater for AvgFloat {
    fn aggregate(&self, mb: &MessageBatch) -> Value {
        let mut sum = 0.0;
        let mut count: f64 = 0.0;
        let messages = mb.get_messages();
        for message in messages {
            match message.get_f64(&self.field) {
                Some(value) => {
                    count += 1.0;
                    sum += value;
                }
                None => {}
            }
        }

        let avg = sum / count;

        Value::from(avg)
    }
}