use anyhow::Result;
use message::MessageBatch;
use tokio::sync::broadcast::Receiver;

pub mod mqtt;
pub mod device;

pub trait Source: Send + Sync {
    fn subscribe(&mut self) -> Result<Receiver<MessageBatch>>;

    fn stop(&self) {}
}
