use crate::aggregate_return;

use super::Aggregater;
use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregation::ItemConf;

struct Min {
    field: String,
    target_field: Option<String>,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Min {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl Aggregater for Min {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut is_float = false;
        let mut min_i = std::i64::MAX;
        let mut min_f = std::f64::MAX;

        for message in mb.get_messages() {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int64(value) => match is_float {
                        true => {
                            if min_f > *value as f64 {
                                min_f = *value as f64
                            }
                        }
                        false => {
                            if min_i > *value {
                                min_i = *value
                            }
                        }
                    },
                    MessageValue::Float64(value) => match is_float {
                        true => {
                            if min_f > *value {
                                min_f = *value
                            }
                        }
                        false => {
                            is_float = true;

                            if min_i as f64 > *value {
                                min_f = min_i as f64
                            } else {
                                min_f = *value
                            }
                        }
                    },
                    _ => {}
                },
                None => {}
            }
        }

        let result = match is_float {
            true => MessageValue::Float64(min_f),
            false => MessageValue::Int64(min_i),
        };

        aggregate_return!(self, result)
    }
}
