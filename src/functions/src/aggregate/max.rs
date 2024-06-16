use super::Aggregater;
use message::MessageBatch;

pub struct Max {
    field: String,
}

impl Max {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(Max { field })
    }
}

impl Aggregater for Max {
    fn aggregate(&self, mb: &MessageBatch) -> json::Value {
        let mut max = std::f64::MIN;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    json::Value::Int8(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    json::Value::Int16(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    json::Value::Int32(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    json::Value::Int64(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    json::Value::UInt8(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    json::Value::UInt16(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    json::Value::UInt32(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    json::Value::UInt64(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    json::Value::Float32(value) => {
                        if *value as f64 > max {
                            max = *value as f64
                        }
                    }
                    json::Value::Float64(value) => {
                        if *value > max {
                            max = *value
                        }
                    }
                    _ => {}
                },
                None => {}
            }
        }

        json::Value::Float64(max)
    }
}
