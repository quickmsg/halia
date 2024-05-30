use anyhow::Result;
use message::MessageBatch;
use tokio::sync::broadcast::Receiver;

pub mod log;

pub trait Sink: Send {
    fn insert_receiver(&mut self, receiver: Receiver<MessageBatch>) -> Result<()>;
}