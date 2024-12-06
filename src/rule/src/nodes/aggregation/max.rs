use crate::aggregate_return;

use super::Aggregater;
use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregation::ItemConf;

struct Max {
    field: String,
    target_field: Option<String>,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Max {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl Aggregater for Max {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut is_float = false;
        let mut max_i = std::i64::MIN;
        let mut max_f = std::f64::MIN;

        for message in mb.get_messages() {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int64(value) => match is_float {
                        true => {
                            if max_f < *value as f64 {
                                max_f = *value as f64
                            }
                        }
                        false => {
                            if max_i < *value {
                                max_i = *value
                            }
                        }
                    },
                    MessageValue::Float64(value) => match is_float {
                        true => {
                            if max_f < *value {
                                max_f = *value
                            }
                        }
                        false => {
                            is_float = true;

                            if (max_i as f64) < *value {
                                max_f = max_i as f64
                            } else {
                                max_f = *value
                            }
                        }
                    },
                    _ => {}
                },
                None => {}
            }
        }

        let result = match is_float {
            true => MessageValue::Float64(max_f),
            false => MessageValue::Int64(max_i),
        };

        aggregate_return!(self, result)
    }
}
