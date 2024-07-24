use message::MessageBatch;

pub mod aggregate;
mod field;
mod filter;
pub mod merge;
pub mod window;

pub trait Function: Send + Sync {
    // 修改消息，根据返回值判断是否要继续流程，为false则消息丢弃
    fn call(&self, message_batch: &mut MessageBatch) -> bool;
}
