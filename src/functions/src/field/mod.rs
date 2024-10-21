use anyhow::Result;
use async_trait::async_trait;
use message::MessageBatch;

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
    fn operate(&self, mb: &mut MessageBatch);
}

pub struct FieldNode {
    operators: Vec<Box<dyn Operator>>,
}

// pub fn new(conf: FilterConf) -> Result<Box<dyn Function>> {
//     todo!()
// }

#[async_trait]
impl Function for FieldNode {
    async fn call(&self, message_batch: &mut message::MessageBatch) -> bool {
        todo!()
    }
}
