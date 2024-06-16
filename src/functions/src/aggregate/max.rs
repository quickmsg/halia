use message::MessageBatch;
use serde_json::Value;

use super::Aggregater;

pub struct MaxInt {
    field: String,
}

impl MaxInt {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(MaxInt { field })
    }
}

impl Aggregater for MaxInt {
    fn aggregate(&self, mb: &MessageBatch) -> Value {
        let mut max = std::i64::MIN;
        let messages = mb.get_messages();
        for message in messages {
            match message.get_i64(&self.field) {
                Some(value) => {
                    max = max.max(value);
                }
                None => {}
            }
        }

        Value::from(max)
    }
}

pub struct MaxFloat {
    field: String,
}

impl MaxFloat {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(MaxFloat { field })
    }
}

impl Aggregater for MaxFloat {
    fn aggregate(&self, mb: &MessageBatch) -> Value {
        let mut max = std::f64::MIN;
        let messages = mb.get_messages();
        for message in messages {
            match message.get_f64(&self.field) {
                Some(value) => {
                    max = max.max(value);
                }
                None => {}
            }
        }

        Value::from(max)
    }
}
