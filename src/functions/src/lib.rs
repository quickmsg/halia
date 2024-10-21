use async_trait::async_trait;
use message::MessageBatch;

pub mod aggregate;
pub mod compress;
pub mod computes;
pub mod field;
pub mod filter;
pub mod log;
pub mod merge;
pub mod metadata;
pub mod type_conversion;
pub mod type_judgment;
pub mod types;
pub mod window;

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
    async fn call(&self, message_batch: &mut MessageBatch) -> bool;
}
