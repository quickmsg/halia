use std::{fmt::Display, io, result};

pub type HaliaResult<T, E = HaliaError> = result::Result<T, E>;

#[derive(Debug)]
pub enum HaliaError {
    NotFound,
    NotFoundGroup,
    ProtocolNotSupported,
    ParseErr,
    IoErr,
    Existed,
    ConfErr,

    DeviceNotFound,
    DeviceRunning,
    DeviceStopped,
    DeviceConnectionError(String),

    Common(String),
}

impl From<std::fmt::Error> for HaliaError {
    fn from(_e: std::fmt::Error) -> Self {
        HaliaError::NotFound
    }
}

impl From<serde_json::Error> for HaliaError {
    fn from(_e: serde_json::Error) -> Self {
        HaliaError::ParseErr
    }
}

impl From<io::Error> for HaliaError {
    fn from(_e: io::Error) -> Self {
        HaliaError::IoErr
    }
}

impl Display for HaliaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HaliaError::NotFound => write!(f, "未找到"),
            HaliaError::ProtocolNotSupported => write!(f, "协议未支持"),
            HaliaError::ParseErr => write!(f, "解析错误"),
            HaliaError::IoErr => write!(f, "IO错误"),
            HaliaError::Existed => write!(f, "已存在"),
            HaliaError::ConfErr => write!(f, "配置错误"),
            HaliaError::NotFoundGroup => todo!(),
            HaliaError::DeviceRunning => write!(f, "设备运行中"),
            HaliaError::DeviceNotFound => todo!(),
            HaliaError::DeviceStopped => todo!(),
            HaliaError::DeviceConnectionError(_) => todo!(),
            HaliaError::Common(_) => todo!(),
        }
    }
}

impl HaliaError {
    pub fn code(&self) -> u16 {
        match self {
            HaliaError::NotFound => 1,
            HaliaError::ProtocolNotSupported => 2,
            HaliaError::ParseErr => 3,
            HaliaError::IoErr => 4,
            HaliaError::Existed => 5,
            HaliaError::ConfErr => 6,
            HaliaError::DeviceRunning => 1006,
            HaliaError::NotFoundGroup => 1005,
            HaliaError::DeviceNotFound => todo!(),
            HaliaError::DeviceStopped => todo!(),
            HaliaError::DeviceConnectionError(_) => todo!(),
            HaliaError::Common(_) => todo!(),
        }
    }
}
