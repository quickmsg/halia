use super::Aggregater;
use message::MessageBatch;

pub struct Min {
    field: String,
}

impl Min {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(Min { field })
    }
}

impl Aggregater for Min {
    fn aggregate(&self, mb: &MessageBatch) -> json::Value {
        let mut min = std::f64::MAX;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    json::Value::Int8(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    json::Value::Int16(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    json::Value::Int32(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    json::Value::Int64(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    json::Value::UInt8(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    json::Value::UInt16(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    json::Value::UInt32(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    json::Value::UInt64(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    json::Value::Float32(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    json::Value::Float64(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    _ => {}
                },
                None => {}
            }
        }

        json::Value::Float64(min)
    }
}
