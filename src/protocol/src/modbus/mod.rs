#![warn(let_underscore)]
#![warn(missing_debug_implementations)]
#![warn(unreachable_pub)]
#![warn(unsafe_code)]
#![warn(unused)]
// Clippy lints
#![warn(clippy::pedantic)]
// Additional restrictions
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::self_named_module_files)]
// Exceptions
#![allow(clippy::enum_glob_use)]
#![allow(clippy::similar_names)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::wildcard_imports)] // TODO
#![allow(clippy::missing_errors_doc)] // TODO

pub mod client;

mod codec;

mod error;
pub use self::error::{Error, ProtocolError};

mod frame;
pub use self::frame::{Exception, ExceptionResponse, FunctionCode, Request, Response};

/// Specialized [`std::result::Result`] type for type-checked responses of the _Modbus_ client API.
///
/// The payload is generic over the response type.
///
/// This [`Result`] type contains 2 layers of errors.
///
/// 1. [`Error`]: An unexpected protocol or network error that occurred during client/server communication.
/// 2. [`Exception`]: An error occurred on the _Modbus_ server.
pub type Result<T> = std::result::Result<std::result::Result<T, Exception>, Error>;

mod service;

pub trait SlaveContext {
    /// Select a slave device for all subsequent outgoing requests.
    fn set_slave(&mut self, slave: u8);
}
