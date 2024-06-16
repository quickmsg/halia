use message::MessageBatch;
use serde_json::Value;
use tracing::debug;

use super::Aggregater;

pub struct SumInt {
    field: String,
}

impl SumInt {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(SumInt { field })
    }
}

impl Aggregater for SumInt {
    fn aggregate(&self, mb: &MessageBatch) -> Value {
        let mut sum = 0;
        let messages = mb.get_messages();
        debug!("messages: {:?}", messages);
        for message in messages {
            debug!("field is: {:?}", self.field);
            match message.get_i64(&self.field) {
                Some(value) => {
                    sum += value;
                }
                None => {}
            }
        }

        Value::from(sum)
    }
}


pub struct SumFloat {
    field: String,
}

impl SumFloat {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(SumFloat { field })
    }
}

impl Aggregater for SumFloat {
    fn aggregate(&self, mb: &MessageBatch) -> Value {
        let mut sum = 0.0;
        let messages = mb.get_messages();
        for message in messages {
            match message.get_f64(&self.field) {
                Some(value) => {
                    sum += value;
                }
                None => {}
            }
        }

        Value::from(sum)
    }
}