use std::{io, result};

use uuid::Uuid;

pub type HaliaResult<T, E = HaliaError> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum HaliaError {
    #[error("{0} id:{1} 不存在")]
    NotFound(String, Uuid),
    #[error("{0}")]
    JsonErr(#[from] serde_json::Error),
    #[error("{0}")]
    Common(String),
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("运行中")]
    Running,
    #[error("已停止")]
    Stopped,
    #[error("引用中，无法删除！")]
    DeleteRefing,
    #[error("有规则引用，运行中！")]
    StopActiveRefing,
}
