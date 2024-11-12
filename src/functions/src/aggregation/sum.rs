use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregation::ItemConf;

use crate::aggregate_return;

use super::Aggregater;

struct Sum {
    field: String,
    target_field: Option<String>,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Sum {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl Aggregater for Sum {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut is_float = false;
        let mut sum_i: i64 = 0;
        let mut sum_f: f64 = 0.0;

        for message in mb.get_messages() {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int64(value) => match is_float {
                        true => {
                            sum_f += *value as f64;
                        }
                        false => {
                            sum_i += *value;
                        }
                    },
                    MessageValue::Float64(value) => match is_float {
                        true => {
                            sum_f += value;
                        }
                        false => {
                            is_float = true;
                            sum_f = sum_i as f64 + value;
                        }
                    },
                    _ => {}
                },
                None => {}
            }
        }

        let result = match is_float {
            true => MessageValue::Float64(sum_f),
            false => MessageValue::Int64(sum_i),
        };

        aggregate_return!(self, result)
    }
}
