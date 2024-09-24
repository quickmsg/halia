use std::{io, result};

pub type HaliaResult<T, E = HaliaError> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum HaliaError {
    #[error("ID{0} 未找到！")]
    NotFound(String),
    #[error("{0}")]
    JsonErr(#[from] serde_json::Error),
    #[error("{0}")]
    Common(String),
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("运行中")]
    Running,
    #[error("{0} 停止!")]
    Stopped(String),
    #[error("引用中，无法删除！")]
    DeleteRefing,
    #[error("运行中，无法删除，请先停止！")]
    DeleteRunning,
    #[error("有规则引用运行中，无法停止！")]
    StopActiveRefing,
    #[error("名称已存在！")]
    NameExists,
    #[error("地址已存在！")]
    AddressExists,
    #[error("连接断开！")]
    Disconnect,
    #[error("{0}")]
    Error(#[from] anyhow::Error),
}
