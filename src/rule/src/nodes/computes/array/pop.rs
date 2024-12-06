use anyhow::Result;

use crate::nodes::{args::Args, computes::Computer};

struct Push {
    field: String,
    target_field: String,
}

pub fn new(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_target_field()?;
    Ok(Box::new(Push {
        field,
        target_field,
    }))
}

impl Computer for Push {
    fn compute(&mut self, message: &mut message::Message) {
        let mut value = match message.get_array(&self.field) {
            Some(mv) => mv.clone(),
            None => return,
        };

        let result = value.pop();
        match result {
            Some(v) => message.add(self.target_field.clone(), v),
            None => message.add(self.target_field.clone(), message::MessageValue::Null),
        }
    }
}
