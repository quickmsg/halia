use async_trait::async_trait;
use message::MessageBatch;

pub mod aggregation;
pub mod computes;
pub mod field;
pub mod filter;
pub mod merge;
pub mod window;

pub mod args;

#[macro_export]
macro_rules! add_or_set_message_value {
    ($self:expr, $message:expr, $value:expr) => {
        match &$self.target_field {
            Some(target_field) => $message.add(target_field.clone(), $value),
            None => $message.set(&$self.field, $value),
        }
    };
}

#[async_trait]
pub trait Function: Send + Sync {
    // 修改消息，根据返回值判断是否要继续流程，为false则消息丢弃
    async fn call(&mut self, message_batch: &mut MessageBatch) -> bool;
}
