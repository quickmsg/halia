use message::MessageBatch;

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
    fn aggregate(&self, mb: &MessageBatch) -> json::Value {
        let mut sum: f64 = 0.0;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    json::Value::Int8(value) => {
                        sum += *value as f64;
                    }
                    json::Value::Int16(value) => {
                        sum += *value as f64;
                    }
                    json::Value::Int32(value) => {
                        sum += *value as f64;
                    }
                    json::Value::Int64(value) => {
                        sum += *value as f64;
                    }
                    json::Value::UInt8(value) => {
                        sum += *value as f64;
                    }
                    json::Value::UInt16(value) => {
                        sum += *value as f64;
                    }
                    json::Value::UInt32(value) => {
                        sum += *value as f64;
                    }
                    json::Value::UInt64(value) => {
                        sum += *value as f64;
                    }
                    json::Value::Float32(value) => {
                        sum += *value as f64;
                    }
                    json::Value::Float64(value) => {
                        sum += value;
                    }
                    _ => {}
                },
                None => {}
            }
        }

        json::Value::Float64(sum)
    }
}
