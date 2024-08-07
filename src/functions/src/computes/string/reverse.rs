use crate::computes::Computer;
use anyhow::Result;
use message::Message;
use serde_json::Value;

pub(crate) struct Reverse {
    field: String,
}

impl Reverse {
    pub(crate) fn new(field: String) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Reverse { field }))
    }
}

impl Computer for Reverse {
    fn compute(&self, message: &Message) -> Option<Value> {
        match message.get_string(&self.field) {
            Some(value) => Some(Value::from(value.chars().rev().collect::<String>())),
            None => None,
        }
    }
    use crate::computes::Computer;
    use anyhow::Result;
    use message::{Message, MessageValue};
    use serde_json::Value;
    use types::rules::functions::ComputerConfItem;
    
    struct Lower {
        field: String,
        target_field: Option<String>,
    }
    
    pub fn new(conf: ComputerConfItem) -> Result<Box<dyn Computer>> {
        Ok(Box::new(Lower {
            field: conf.field,
            target_field: conf.target_field,
        }))
    }
    
    impl Computer for Lower {
        fn compute(&self, message: &mut Message) {
            let value = match message.get(&self.field) {
                Some(mv) => match mv {
                    MessageValue::String(s) => MessageValue::String(s.to_lowercase()),
                    _ => MessageValue::Null,
                },
                None => MessageValue::Null,
            };
    
            match &self.target_field {
                Some(target_field) => message.add(target_field.clone(), value),
                None => message.set(&self.field, value),
            }
        }
    }
    }