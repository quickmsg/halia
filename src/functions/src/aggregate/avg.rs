use message::MessageBatch;

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
    fn aggregate(&self, mb: &MessageBatch) -> json::Value {
        let mut sum: f64 = 0.0;
        let mut count = 0;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    json::Value::Int8(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    json::Value::Int16(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    json::Value::Int32(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    json::Value::Int64(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    json::Value::Uint8(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    json::Value::Uint16(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    json::Value::Uint32(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    json::Value::Uint64(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    json::Value::Float32(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    json::Value::Float64(value) => {
                        sum += value;
                        count += 1;
                    }
                    _ => {}
                },
                None => {}
            }
        }

        if count > 0 {
            json::Value::Float64(sum / count as f64)
        } else {
            json::Value::Float64(0.0)
        }
    }
}
