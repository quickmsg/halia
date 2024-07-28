mod client;
mod tcp;

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

    /// Modbus Function Code: `23` (`0x17`).
    ReadWriteMultipleRegisters,
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
            FunctionCode::ReadWriteMultipleRegisters => 0x17,
        }
    }
}

pub struct Context {
    // TCP only
    transcation_id: u16,
}

impl Context {
    pub fn new() -> Self {
        Context { transcation_id: 0 }
    }

    pub fn encode(&mut self) {}
}
