use super::Aggregater;
use message::{MessageValue, MessageBatch};

pub struct Min {
    field: String,
}

impl Min {
    pub fn new(field: String) -> Box<dyn Aggregater> {
        Box::new(Min { field })
    }
}

impl Aggregater for Min {
    fn aggregate(&self, mb: &MessageBatch) -> MessageValue {
        let mut min = std::f64::MAX;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int8(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Int16(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Int32(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Int64(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Uint8(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Uint16(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Uint32(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Uint64(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Float32(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    MessageValue::Float64(value) => {
                        if min > *value as f64 {
                            min = *value as f64
                        }
                    }
                    _ => {}
                },
                None => {}
            }
        }

        MessageValue::Float64(min)
    }
}
