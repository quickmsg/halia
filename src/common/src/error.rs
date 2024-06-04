use std::{fmt::Display, io, result};

pub type Result<T, E = HaliaError> = result::Result<T, E>;

#[derive(Debug)]
pub enum HaliaError {
    NotFound,
    ProtocolNotSupported,
    ParseErr,
    IoErr,
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
        write!(f, "todo")
    }
}

impl HaliaError {
    pub fn code(&self) -> u8 {
        match self {
            HaliaError::NotFound => 1,
            HaliaError::ProtocolNotSupported => 2,
            HaliaError::ParseErr => 3,
            HaliaError::IoErr => 4,
        }
    }
}
