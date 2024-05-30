use anyhow::Result;
use message::MessageBatch;
use tokio::sync::broadcast::Receiver;

pub mod mqtt;

pub trait Source: Send {
    fn subscribe(&mut self) -> Result<Receiver<MessageBatch>>;

    fn stop(&self) {}
}
