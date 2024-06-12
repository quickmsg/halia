use anyhow::Result;
use message::MessageBatch;
use tokio::sync::broadcast::Receiver;
use types::sink::{ListSinkResp, ReadSinkResp};

pub mod log;

pub trait Sink: Send + Sync {
    fn get_detail(&self) -> Result<ReadSinkResp>;

    fn get_info(&self) -> Result<ListSinkResp>;

    fn insert_receiver(&mut self, receiver: Receiver<MessageBatch>) -> Result<()>;
}
