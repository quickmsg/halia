use std::io;

pub mod tcp;

#[derive(PartialEq, Clone)]
pub enum FunctionCode {
    /// Modbus Function Code: `01` (`0x01`).
    ReadCoils,
    /// Modbus Function Code: `02` (`0x02`).
    ReadDiscreteInputs,

    /// Modbus Function Code: `03` (`0x03`).
    ReadHoldingRegisters,
    /// Modbus Function Code: `04` (`0x04`).
    ReadInputRegisters,

    /// Modbus Function Code: `05` (`0x05`).
    WriteSingleCoil,
    /// Modbus Function Code: `06` (`0x06`).
    WriteSingleRegister,

    /// Modbus Function Code: `16` (`0x10`).
    WriteMultipleRegisters,

    /// Modbus Function Code: `22` (`0x16`).
    MaskWriteRegister,
}

impl From<FunctionCode> for u8 {
    fn from(value: FunctionCode) -> Self {
        match value {
            FunctionCode::ReadCoils => 0x01,
            FunctionCode::ReadDiscreteInputs => 0x02,
            FunctionCode::ReadHoldingRegisters => 0x03,
            FunctionCode::ReadInputRegisters => 0x04,
            FunctionCode::WriteSingleCoil => 0x05,
            FunctionCode::WriteSingleRegister => 0x06,
            FunctionCode::WriteMultipleRegisters => 0x10,
            FunctionCode::MaskWriteRegister => 0x16,
        }
    }
}

pub enum Error {
    Transport(io::Error),
    Protocol(ProtocolError),
}

pub enum ProtocolError {
    EmptyResp,
    DataTooSmall,
    TranscationIdMismatch,
    ProtocolIdErr,
    UnitIdMismatch,
}

pub trait Context: Send + Sync {
    async fn read(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], Error>;

    async fn write(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        value: &[u8],
    ) -> Result<(), Error>;
}
