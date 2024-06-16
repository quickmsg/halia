use message::MessageBatch;
use serde_json::Value;
use super::Aggregater;

pub struct MinInt {
    field: String,
}

impl MinInt {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(MinInt { field })
    }
}

impl Aggregater for MinInt {
    fn aggregate(&self, mb: &MessageBatch) -> Value {
        let mut min = std::i64::MAX;
        let messages = mb.get_messages();
        for message in messages {
            match message.get_i64(&self.field) {
                Some(value) => {
                    min = min.min(value);
                }
                None => {}
            }
        }

        Value::from(min)
    }
}

pub struct MinFloat {
    field: String,
}

impl MinFloat {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(MinFloat { field })
    }
}

impl Aggregater for MinFloat {
    fn aggregate(&self, mb: &MessageBatch) -> Value {
        let mut min = std::f64::MAX;
        let messages = mb.get_messages();
        for message in messages {
            match message.get_f64(&self.field) {
                Some(value) => {
                    min = min.min(value);
                }
                None => {}
            }
        }

        Value::from(min)
    }
}
