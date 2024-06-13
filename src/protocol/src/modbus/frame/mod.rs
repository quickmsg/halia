pub(crate) mod rtu;
pub(crate) mod tcp;

use std::{
    borrow::Cow,
    error,
    fmt::{self, Display},
};

use bytes::Bytes;

/// A Modbus function code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionCode {
    /// Modbus Function Code: `01` (`0x01`).
    ReadCoils,
    /// Modbus Function Code: `02` (`0x02`).
    ReadDiscreteInputs,

    /// Modbus Function Code: `05` (`0x05`).
    WriteSingleCoil,
    /// Modbus Function Code: `06` (`0x06`).
    WriteSingleRegister,

    /// Modbus Function Code: `03` (`0x03`).
    ReadHoldingRegisters,
    /// Modbus Function Code: `04` (`0x04`).
    ReadInputRegisters,

    /// Modbus Function Code: `15` (`0x0F`).
    WriteMultipleCoils,
    /// Modbus Function Code: `16` (`0x10`).
    WriteMultipleRegisters,

    /// Modbus Function Code: `22` (`0x16`).
    MaskWriteRegister,

    /// Modbus Function Code: `23` (`0x17`).
    ReadWriteMultipleRegisters,

    /// Custom Modbus Function Code.
    Custom(u8),
}

impl FunctionCode {
    /// Create a new [`FunctionCode`] with `value`.
    #[must_use]
    pub const fn new(value: u8) -> Self {
        match value {
            0x01 => FunctionCode::ReadCoils,
            0x02 => FunctionCode::ReadDiscreteInputs,
            0x05 => FunctionCode::WriteSingleCoil,
            0x06 => FunctionCode::WriteSingleRegister,
            0x03 => FunctionCode::ReadHoldingRegisters,
            0x04 => FunctionCode::ReadInputRegisters,
            0x0F => FunctionCode::WriteMultipleCoils,
            0x10 => FunctionCode::WriteMultipleRegisters,
            0x16 => FunctionCode::MaskWriteRegister,
            0x17 => FunctionCode::ReadWriteMultipleRegisters,
            code => FunctionCode::Custom(code),
        }
    }

    /// Get the [`u8`] value of the current [`FunctionCode`].
    #[must_use]
    pub const fn value(self) -> u8 {
        match self {
            FunctionCode::ReadCoils => 0x01,
            FunctionCode::ReadDiscreteInputs => 0x02,
            FunctionCode::WriteSingleCoil => 0x05,
            FunctionCode::WriteSingleRegister => 0x06,
            FunctionCode::ReadHoldingRegisters => 0x03,
            FunctionCode::ReadInputRegisters => 0x04,
            FunctionCode::WriteMultipleCoils => 0x0F,
            FunctionCode::WriteMultipleRegisters => 0x10,
            FunctionCode::MaskWriteRegister => 0x16,
            FunctionCode::ReadWriteMultipleRegisters => 0x17,
            FunctionCode::Custom(code) => code,
        }
    }
}

impl Display for FunctionCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value().fmt(f)
    }
}

/// A request represents a message from the client (master) to the server (slave).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request<'a> {
    /// A request to read multiple u8s.
    /// The first parameter is the u16 of the first u8 to read.
    /// The second parameter is the number of u8s to read.
    ReadCoils(u16, u16),

    /// A request to read multiple discrete inputs
    /// The first parameter is the u16 of the first discrete input to read.
    /// The second parameter is the number of discrete inputs to read.
    ReadDiscreteInputs(u16, u16),

    /// A request to write a single u8.
    /// The first parameter is the u16 of the u8.
    /// The second parameter is the value to write to the u8.
    WriteSingleCoil(u16, u8),

    /// A request to write multiple u8s.
    /// The first parameter is the u16 of the first u8 to write.
    /// The second parameter is the vector of values to write to the u8s.
    WriteMultipleCoils(u16, Cow<'a, [u8]>),

    /// A request to read multiple input registers.
    /// The first parameter is the u16 of the first input register to read.
    /// The second parameter is the number of input registers to read.
    ReadInputRegisters(u16, u16),

    /// A request to read multiple holding registers.
    /// The first parameter is the u16 of the first holding register to read.
    /// The second parameter is the number of holding registers to read.
    ReadHoldingRegisters(u16, u16),

    /// A request to write a single register.
    /// The first parameter is the u16 of the register to read.
    /// The second parameter is the value to write to the register.
    WriteSingleRegister(u16, Cow<'a, [u8]>),

    /// A request to write to multiple registers.
    /// The first parameter is the u16 of the first register to write.
    /// The second parameter is the vector of values to write to the registers.
    WriteMultipleRegisters(u16, Cow<'a, [u8]>),

    /// A request to set or clear individual bits of a holding register.
    /// The first parameter is the u16 of the holding register.
    /// The second parameter is the AND mask.
    /// The third parameter is the OR mask.
    MaskWriteRegister(u16, u16, u16),

    /// A request to simultaneously read multiple registers and write multiple registers.
    /// The first parameter is the u16 of the first register to read.
    /// The second parameter is the number of registers to read.
    /// The third parameter is the u16 of the first register to write.
    /// The fourth parameter is the vector of values to write to the registers.
    ReadWriteMultipleRegisters(u16, u16, u16, Cow<'a, [u8]>),

    /// A raw Modbus request.
    /// The first parameter is the Modbus function code.
    /// The second parameter is the raw bytes of the request.
    Custom(u8, Cow<'a, [u8]>),

    /// A poison pill for stopping the client service and to release
    /// the underlying transport, e.g. for disconnecting from an
    /// exclusively used serial port.
    ///
    /// This is an ugly workaround, because `tokio-proto` does not
    /// provide other means to gracefully shut down the `ClientService`.
    /// Otherwise the bound transport is never freed as long as the
    /// executor is active, even when dropping the Modbus client
    /// context.
    Disconnect,
}

impl<'a> Request<'a> {
    /// Converts the request into an owned instance with `'static'` lifetime.
    #[must_use]
    pub fn into_owned(self) -> Request<'static> {
        use Request::*;

        match self {
            ReadCoils(addr, qty) => ReadCoils(addr, qty),
            ReadDiscreteInputs(addr, qty) => ReadDiscreteInputs(addr, qty),
            WriteSingleCoil(addr, coil) => WriteSingleCoil(addr, coil),
            WriteMultipleCoils(addr, u8s) => WriteMultipleCoils(addr, Cow::Owned(u8s.into_owned())),
            ReadInputRegisters(addr, qty) => ReadInputRegisters(addr, qty),
            ReadHoldingRegisters(addr, qty) => ReadHoldingRegisters(addr, qty),
            WriteSingleRegister(addr, data) => {
                WriteSingleRegister(addr, Cow::Owned(data.into_owned()))
            }
            WriteMultipleRegisters(addr, u8s) => {
                WriteMultipleRegisters(addr, Cow::Owned(u8s.into_owned()))
            }
            MaskWriteRegister(addr, and_mask, or_mask) => {
                MaskWriteRegister(addr, and_mask, or_mask)
            }
            ReadWriteMultipleRegisters(addr, qty, write_addr, u8s) => {
                ReadWriteMultipleRegisters(addr, qty, write_addr, Cow::Owned(u8s.into_owned()))
            }
            Custom(func, bytes) => Custom(func, Cow::Owned(bytes.into_owned())),
            Disconnect => Disconnect,
        }
    }

    /// Get the [`FunctionCode`] of the [`Request`].
    #[must_use]
    pub const fn function_code(&self) -> FunctionCode {
        use Request::*;

        match self {
            ReadCoils(_, _) => FunctionCode::ReadCoils,
            ReadDiscreteInputs(_, _) => FunctionCode::ReadDiscreteInputs,

            WriteSingleCoil(_, _) => FunctionCode::WriteSingleCoil,
            WriteMultipleCoils(_, _) => FunctionCode::WriteMultipleCoils,

            ReadInputRegisters(_, _) => FunctionCode::ReadInputRegisters,
            ReadHoldingRegisters(_, _) => FunctionCode::ReadHoldingRegisters,

            WriteSingleRegister(_, _) => FunctionCode::WriteSingleRegister,
            WriteMultipleRegisters(_, _) => FunctionCode::WriteMultipleRegisters,

            MaskWriteRegister(_, _, _) => FunctionCode::MaskWriteRegister,

            ReadWriteMultipleRegisters(_, _, _, _) => FunctionCode::ReadWriteMultipleRegisters,

            Custom(code, _) => FunctionCode::Custom(*code),

            Disconnect => unreachable!(),
        }
    }
}

/// The data of a successful request.
///
/// Readu8s/ReadDiscreteInputs: The length of the result Vec is always a
/// multiple of 8. Only the values of the first bits/u8s that have actually
/// been requested are defined. The value of the remaining bits depend on the
/// server implementation and those u8s should be should be ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    ReadCoils(Vec<u8>),
    ReadDiscreteInputs(Vec<u8>),
    WriteSingleCoil(u16, u8),
    WriteMultipleCoils(u16, u16),

    /// Response to a ReadInputRegisters request
    /// The parameter contains the register values that have been read
    ReadInputRegisters(Vec<u8>),

    /// Response to a ReadHoldingRegisters request
    /// The parameter contains the register values that have been read
    ReadHoldingRegisters(Vec<u8>),

    /// Response to a WriteSingleRegister request
    /// The first parameter contains the u16 of the register that has been written to
    /// The second parameter contains the value that has been written to the register at the given u16
    WriteSingleRegister(u16, u16),

    /// Response to a WriteMultipleRegisters request
    /// The first parameter contains the u16 at the start of the register range that has been written to
    /// The second parameter contains the amount of register that have been written
    WriteMultipleRegisters(u16, u16),

    /// Response MaskWriteRegister
    /// The first parameter is the u16 of the holding register.
    /// The second parameter is the AND mask.
    /// The third parameter is the OR mask.
    MaskWriteRegister(u16, u16, u16),

    /// Response to a ReadWriteMultipleRegisters request
    /// The parameter contains the register values that have been read as part of the read instruction
    ReadWriteMultipleRegisters(Vec<u8>),

    /// Response to a raw Modbus request
    /// The first parameter contains the returned Modbus function code
    /// The second parameter contains the bytes read following the function code
    Custom(u8, Bytes),
}

impl Response {
    /// Get the [`FunctionCode`] of the [`Response`].
    #[must_use]
    pub const fn function_code(&self) -> FunctionCode {
        use Response::*;

        match self {
            ReadCoils(_) => FunctionCode::ReadCoils,
            ReadDiscreteInputs(_) => FunctionCode::ReadDiscreteInputs,

            WriteSingleCoil(_, _) => FunctionCode::WriteSingleCoil,
            WriteMultipleCoils(_, _) => FunctionCode::WriteMultipleCoils,

            ReadInputRegisters(_) => FunctionCode::ReadInputRegisters,
            ReadHoldingRegisters(_) => FunctionCode::ReadHoldingRegisters,

            WriteSingleRegister(_, _) => FunctionCode::WriteSingleRegister,
            WriteMultipleRegisters(_, _) => FunctionCode::WriteMultipleRegisters,

            MaskWriteRegister(_, _, _) => FunctionCode::MaskWriteRegister,

            ReadWriteMultipleRegisters(_) => FunctionCode::ReadWriteMultipleRegisters,

            Custom(code, _) => FunctionCode::Custom(*code),
        }
    }
}

/// A server (slave) exception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Exception {
    IllegalFunction = 0x01,
    IllegalDataAddress = 0x02,
    IllegalDataValue = 0x03,
    ServerDeviceFailure = 0x04,
    Acknowledge = 0x05,
    ServerDeviceBusy = 0x06,
    MemoryParityError = 0x08,
    GatewayPathUnavailable = 0x0A,
    GatewayTargetDevice = 0x0B,
}

impl From<Exception> for u8 {
    fn from(from: Exception) -> Self {
        from as u8
    }
}

impl Exception {
    pub(crate) fn description(&self) -> &str {
        use super::frame::Exception::*;

        match *self {
            IllegalFunction => "Illegal function",
            IllegalDatau16 => "Illegal data u16",
            IllegalDataValue => "Illegal data value",
            ServerDeviceFailure => "Server device failure",
            Acknowledge => "Acknowledge",
            ServerDeviceBusy => "Server device busy",
            MemoryParityError => "Memory parity error",
            GatewayPathUnavailable => "Gateway path unavailable",
            GatewayTargetDevice => "Gateway target device failed to respond",
        }
    }
}

/// A server (slave) exception response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExceptionResponse {
    pub function: FunctionCode,
    pub exception: Exception,
}

/// Represents a message from the client (slave) to the server (master).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestPdu<'a>(pub(crate) Request<'a>);

impl<'a> From<Request<'a>> for RequestPdu<'a> {
    fn from(from: Request<'a>) -> Self {
        RequestPdu(from)
    }
}

impl<'a> From<RequestPdu<'a>> for Request<'a> {
    fn from(from: RequestPdu<'a>) -> Self {
        from.0
    }
}

/// Represents a message from the server (slave) to the client (master).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResponsePdu(pub(crate) Result<Response, ExceptionResponse>);

impl From<Response> for ResponsePdu {
    fn from(from: Response) -> Self {
        ResponsePdu(Ok(from))
    }
}

impl From<ExceptionResponse> for ResponsePdu {
    fn from(from: ExceptionResponse) -> Self {
        ResponsePdu(Err(from))
    }
}

impl From<Result<Response, ExceptionResponse>> for ResponsePdu {
    fn from(from: Result<Response, ExceptionResponse>) -> Self {
        ResponsePdu(from.map(Into::into).map_err(Into::into))
    }
}

impl From<ResponsePdu> for Result<Response, ExceptionResponse> {
    fn from(from: ResponsePdu) -> Self {
        from.0
    }
}

impl fmt::Display for Exception {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

impl error::Error for Exception {
    fn description(&self) -> &str {
        self.description()
    }
}

impl fmt::Display for ExceptionResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Modbus function {}: {}", self.function, self.exception)
    }
}

impl error::Error for ExceptionResponse {
    fn description(&self) -> &str {
        self.exception.description()
    }
}