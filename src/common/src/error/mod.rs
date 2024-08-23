use std::{io, result};

pub type HaliaResult<T, E = HaliaError> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum HaliaError {
    #[error("{0} 未找到！")]
    NotFound(String),
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
    #[error("名称已存在！")]
    NameExists,
    #[error("地址已存在！")]
    AddressExists,
}
