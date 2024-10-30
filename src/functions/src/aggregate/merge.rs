use std::collections::HashMap;

use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

use super::Aggregater;

struct Merge {
    field: String,
    all: bool,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    let all = match conf.field.as_str() {
        "*" => true,
        _ => false,
    };
    Box::new(Merge {
        field: conf.field,
        all,
    })
}

impl Aggregater for Merge {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut resp_value = HashMap::new();
        for message in mb.get_messages() {
            match &self.all {
                true => {
                    if let Some(obj) = message.get_obj() {
                        for (key, value) in obj.iter() {
                            resp_value.insert(key.clone(), value.clone());
                        }
                    }
                }
                false => {
                    message.get(&self.field).map(|value| match value {
                        MessageValue::Object(hash_map) => {
                            for (key, value) in hash_map.iter() {
                                resp_value.insert(key.clone(), value.clone());
                            }
                        }
                        _ => {}
                    });
                }
            }
        }

        (self.field.clone(), MessageValue::Object(resp_value))
    }
}
