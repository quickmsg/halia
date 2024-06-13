use anyhow::Result;
use async_trait::async_trait;
use common::error::HaliaResult;
use message::MessageBatch;
use tokio::sync::broadcast::{self, Receiver};
use types::source::ListSourceResp;

pub mod device;
pub mod mqtt;

#[async_trait]
pub trait Source: Send + Sync {
    async fn subscribe(&mut self) -> HaliaResult<broadcast::Receiver<MessageBatch>>;

    fn get_info(&self) -> Result<ListSourceResp>;

    fn stop(&self) {}
}
