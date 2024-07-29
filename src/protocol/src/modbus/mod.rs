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

    /// Modbus Function Code: `15` (`0x0F`).
    WriteMultipleCoils,
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
            FunctionCode::WriteMultipleCoils => 0x0F,
            FunctionCode::WriteMultipleRegisters => 0x10,
            FunctionCode::MaskWriteRegister => 0x16,
        }
    }
}

pub enum ProtocolError {
    EmptyResp,
    DataTooSmall,
    TranscationIdMismatch,
    ProtocolIdErr,
    UnitIdMismatch,
}

pub trait Context {
    fn get_buf(&mut self) -> &mut [u8];
    fn encode_read(
        &mut self,
        slave: u8,
        addr: u16,
        function_code: FunctionCode,
        quantity: u16,
    ) -> &[u8];
    fn decode_read(&mut self, n: usize) -> Result<&mut [u8], ProtocolError>;
    fn encode_write(
        &mut self,
        slave: u8,
        addr: u16,
        function_code: FunctionCode,
        value: &[u8],
    ) -> &[u8];
}
