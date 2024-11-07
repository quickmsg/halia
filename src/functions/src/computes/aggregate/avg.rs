use message::{MessageBatch, MessageValue};
use types::rules::functions::aggregate::ItemConf;

use super::Aggregater;

struct Avg {
    field: String,
}

pub(crate) fn new(conf: ItemConf) -> Box<dyn Aggregater> {
    Box::new(Avg { field: conf.field })
}

impl Aggregater for Avg {
    fn aggregate(&self, mb: &MessageBatch) -> (String, MessageValue) {
        let mut sum: f64 = 0.0;
        let mut count = 0;
        let messages = mb.get_messages();
        for message in messages {
            match message.get(&self.field) {
                Some(value) => match value {
                    MessageValue::Int64(value) => {
                        sum += *value as f64;
                        count += 1;
                    }
                    MessageValue::Float64(value) => {
                        sum += value;
                        count += 1;
                    }
                    _ => {}
                },
                None => {}
            }
        }

        let value = if count > 0 {
            MessageValue::Float64(sum / count as f64)
        } else {
            MessageValue::Float64(0.0)
        };

        (self.field.clone(), value)
    }
}

// #[cfg(test)]
// mod tests {
//     use message::{Message, MessageBatch, MessageValue};
//     use types::rules::functions::aggregate::ItemConf;

//     use crate::aggregate::avg::new;

//     #[test]
//     fn avg() {
//         let mut message_batch = MessageBatch::default();

//         let mut message = Message::default();
//         message.add("field1".to_string(), MessageValue::Int64(1));
//         message_batch.push_message(message);

//         let mut message = Message::default();
//         message.add("field1".to_string(), MessageValue::Int64(2));
//         message_batch.push_message(message);

//         let mut message = Message::default();
//         message.add("field1".to_string(), MessageValue::Int64(4));
//         message_batch.push_message(message);

//         let avg = new(ItemConf {
//             typ: types::rules::functions::aggregate::Type::Avg,
//             field: "field1".to_owned(),
//             target_field: "avg".to_owned(),
//         });
//         let (field, value) = avg.aggregate(&message_batch);
//         assert_eq!(field, "field1");
//         assert_eq!(value, MessageValue::Float64((1 + 2 + 4) as f64 / 3 as f64));
//     }
// }
