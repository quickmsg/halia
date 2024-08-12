use anyhow::Result;
use message::Message;
use types::rules::functions::FilterConf;

use crate::Function;

// pub mod convert;
// pub mod insert;
// pub mod except;
// pub mod name;
// pub mod remove;
pub mod r#move;
// pub mod select;
// pub mod watermark;

pub(crate) trait Operator: Sync + Send {
    fn operate(&self, msg: &mut Message);
}

pub struct FieldNode {
    operators: Vec<Box<dyn Operator>>,
}

pub fn new(conf: FilterConf) -> Result<Box<dyn Function>> {
    todo!()
}

impl Function for FieldNode {
    fn call(&self, message_batch: &mut message::MessageBatch) -> bool {
        todo!()
    }
}