use std::io;

use async_trait::async_trait;
use thiserror::Error;

pub(crate) mod pdu;
pub mod rtu;
pub mod tcp;

// 协议使用大端编码方式

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

#[derive(Debug, Error)]
pub enum Exception {
    #[error("Illegal funcion")]
    IllegalFunction = 0x01,
    #[error("Illegal data address")]
    IllegalDataAddress = 0x02,
    #[error("Illegal data value")]
    IllegalDataValue = 0x03,
    #[error("Server device failure")]
    ServerDeviceFailure = 0x04,
    #[error("Acknowledge")]
    Acknowledge = 0x05,
    #[error("Server device busy")]
    ServerDeviceBusy = 0x06,
    #[error("Memory parity error")]
    MemoryParityError = 0x08,
    #[error("Gateway path unavailable")]
    GatewayPathUnavailable = 0x0A,
    #[error("Gateway target device failed to respond")]
    GatewayTargetDevice = 0x0B,
    #[error("未知错误")]
    UnknowException,
}

#[derive(Error, Debug)]
pub enum ModbusError {
    #[error("网络错误")]
    Transport(#[from] io::Error),
    #[error("协议错误")]
    Protocol(#[from] ProtocolError),
    #[error("异常")]
    Exception(#[from] Exception),
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("返回为空")]
    EmptyResp,
    #[error("返回数据长度过小")]
    DataTooSmall,
    #[error("事物ID不匹配")]
    TranscationIdMismatch,
    #[error("协议ID错误")]
    ProtocolIdErr,
    #[error("unitid不匹配")]
    UnitIdMismatch,
    #[error("功能码不匹配")]
    FunctionCodeMismatch,
}

#[async_trait]
pub trait Context: Send {
    async fn read_coils(
        &mut self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError>;

    async fn read_discrete_inputs(
        &mut self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError>;

    async fn read_holding_registers(
        &mut self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError>;

    async fn read_input_registers(
        &mut self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError>;

    async fn write_single_coil(
        &mut self,
        slave: u8,
        addr: u16,
        value: bool,
    ) -> Result<(), ModbusError>;

    async fn write_single_register(&mut self, slave: u8, addr: u16) -> Result<(), ModbusError>;

    async fn write_multiple_registers(&mut self, slave: u8, addr: u16) -> Result<(), ModbusError>;

    async fn mask_write_register(&mut self, slave: u8, addr: u16) -> Result<(), ModbusError>;
}

pub(crate) fn encode_u16(data: u16) -> (u8, u8) {
    ((data >> 8) as u8, (data & 0x00FF) as u8)
}

pub(crate) fn decode_u16(data0: u8, data1: u8) -> u16 {
    (data0 as u16) << 8 | (data1 as u16)
}
