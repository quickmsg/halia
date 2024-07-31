use std::io;

use thiserror::Error;

pub mod rtu;
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

#[derive(Error, Debug)]
pub enum ModbusError {
    #[error("网络错误")]
    Transport(#[from] io::Error),
    #[error("协议错误")]
    Protocol(ProtocolError),
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("返回结构体为空")]
    EmptyResp,
    #[error("返回数据长度过小")]
    DataTooSmall,
    #[error("事物ID不匹配")]
    TranscationIdMismatch,
    #[error("协议ID错误")]
    ProtocolIdErr,
    #[error("unitid不匹配")]
    UnitIdMismatch,
}

pub trait Context {
    fn read(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> impl std::future::Future<Output = Result<&mut [u8], ModbusError>> + Send;

    fn write(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        value: &[u8],
    ) -> impl std::future::Future<Output = Result<(), ModbusError>> + Send;
}

pub(crate) fn encode_u16(data: u16) -> (u8, u8) {
    ((data >> 8) as u8, (data & 0x00FF) as u8)
}

pub(crate) fn decode_u16(data0: u8, data1: u8) -> u16 {
    (data0 as u16) << 8 | (data1 as u16)
}
