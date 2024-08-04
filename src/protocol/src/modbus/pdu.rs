use super::{encode_u16, Exception, ModbusError, ProtocolError};

// function code: 1 Byte 0x01
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of coilds: 2 Bytes 1 to 2000(0x7D0)
pub fn encode_read_coils(buffer: &mut [u8], addr: u16, quantity: u16) -> u16 {
    buffer[0] = 0x01;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(quantity);
    5
}

// function code: 1 Byte 0x01
// byte count: 1 Byte N
// coil status: n byte, n = N or N+1
// N = quantity / 8, 如果quantity = 0; N = N+1
// Error
// function code: 1 Byte Function code + 0x80
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_read_coils(buffer: &mut [u8]) -> Result<&mut [u8], ModbusError> {
    if buffer[0] == 0x81 {
        match buffer[1] {
            1 => {
                return Err(Exception::IllegalFunction.into());
            }
            2 => {
                return Err(Exception::IllegalDataAddress.into());
            }
            3 => {
                return Err(Exception::IllegalDataValue.into());
            }
            4 => return Err(Exception::ServerDeviceFailure.into()),
            _ => {
                return Err(Exception::UnknowException.into());
            }
        }
    }
    if buffer[0] != 0x01 {
        return Err(ProtocolError::FunctionCodeMismatch.into());
    }

    let len = buffer[1] as usize;

    Ok(&mut buffer[2..2 + len])
}

// function code: 1 Byte 0x02
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of inputs: 2 Bytes 1 to 2000(0x7D0)
pub fn encode_read_discrete_inputs(buffer: &mut [u8], addr: u16, quantity: u16) -> u16 {
    buffer[0] = 0x02;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(quantity);
    5
}

// function code: 1 Byte 0x02
// byte count: 1 Byte N
// input status: N * 1 Byte
// Error
// function code: 1 Byte Function code + 0x80 , 0x82
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_read_discrete_inputs(buffer: &mut [u8]) -> Result<&mut [u8], ModbusError> {
    if buffer[0] == 0x82 {
        match buffer[1] {
            1 => {
                return Err(Exception::IllegalFunction.into());
            }
            2 => {
                return Err(Exception::IllegalDataAddress.into());
            }
            3 => {
                return Err(Exception::IllegalDataValue.into());
            }
            4 => return Err(Exception::ServerDeviceFailure.into()),
            _ => {
                return Err(Exception::UnknowException.into());
            }
        }
    }
    if buffer[0] != 0x02 {
        return Err(ProtocolError::FunctionCodeMismatch.into());
    }

    let len = buffer[1] as usize;
    Ok(&mut buffer[2..2 + len])
}

// function code: 1 Byte 0x03
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of registers: 2 Bytes 1 to 125(0x7D)
pub fn encode_read_holding_registers(buffer: &mut [u8], addr: u16, quantity: u16) -> u16 {
    buffer[0] = 0x03;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(quantity);
    5
}

// function code: 1 Byte 0x03
// byte count: 2 * N
// input status: N * 2 Bytes
// Error
// function code: 1 Byte Function code + 0x80 , 0x83
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_read_holding_registers(buffer: &mut [u8]) -> Result<&mut [u8], ModbusError> {
    if buffer[0] == 0x83 {
        match buffer[1] {
            1 => {
                return Err(Exception::IllegalFunction.into());
            }
            2 => {
                return Err(Exception::IllegalDataAddress.into());
            }
            3 => {
                return Err(Exception::IllegalDataValue.into());
            }
            4 => return Err(Exception::ServerDeviceFailure.into()),
            _ => {
                return Err(Exception::UnknowException.into());
            }
        }
    }

    if buffer[0] != 0x03 {
        return Err(ProtocolError::FunctionCodeMismatch.into());
    }

    let len = buffer[2] as usize;
    Ok(&mut buffer[2..2 + len])
}

// function code: 1 Byte 0x04
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of input registers: 2 Bytes 1 to 125(0x7D)
pub fn encode_read_input_registers(buffer: &mut [u8], addr: u16, quantity: u16) -> u16 {
    buffer[0] = 0x04;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(quantity);
    5
}

// function code: 1 Byte 0x04
// byte count: 2 * N
// input status: N * 2 Bytes
// Error
// function code: 1 Byte Function code + 0x80 , 0x84
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_read_input_registers(buffer: &mut [u8]) -> Result<&mut [u8], ModbusError> {
    if buffer[0] == 0x84 {
        match buffer[1] {
            1 => {
                return Err(Exception::IllegalFunction.into());
            }
            2 => {
                return Err(Exception::IllegalDataAddress.into());
            }
            3 => {
                return Err(Exception::IllegalDataValue.into());
            }
            4 => return Err(Exception::ServerDeviceFailure.into()),
            _ => {
                return Err(Exception::UnknowException.into());
            }
        }
    }

    if buffer[0] != 0x04 {
        return Err(ProtocolError::FunctionCodeMismatch.into());
    }

    let len = buffer[2] as usize;
    Ok(&mut buffer[2..2 + len])
}

// function code: 1 Byte 0x05
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of input registers: 2 Bytes 0x0000 or 0xFF00
pub fn encode_write_single_coil(buffer: &mut [u8], addr: u16, value: Vec<u8>) -> u16 {
    buffer[0] = 0x05;
    (buffer[1], buffer[2]) = encode_u16(addr);
    buffer[3] = value[0];
    buffer[4] = value[1];
    5
}

// function code: 1 Byte 0x05
// output address: 2 Bytes 0x0000 to 0xFFFF
// output value: 2 Bytes
// Error
// function code: 1 Byte Function code + 0x80 , 0x85
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_write_single_coil(buffer: &[u8]) -> Result<(), ModbusError> {
    if buffer[0] == 0x85 {
        match buffer[1] {
            1 => {
                return Err(Exception::IllegalFunction.into());
            }
            2 => {
                return Err(Exception::IllegalDataAddress.into());
            }
            3 => {
                return Err(Exception::IllegalDataValue.into());
            }
            4 => return Err(Exception::ServerDeviceFailure.into()),
            _ => {
                return Err(Exception::UnknowException.into());
            }
        }
    }

    if buffer[0] != 0x05 {
        return Err(ProtocolError::FunctionCodeMismatch.into());
    }

    // TODO

    Ok(())
}

// function code: 1 Byte 0x06
// starting address: 2 Bytes 0x0000 to 0xFFFF
// register value: 2 Bytes 0x0000 or 0xFFFF
pub fn encode_write_single_register(buffer: &mut [u8], addr: u16, data: Vec<u8>) -> u16 {
    buffer[0] = 0x06;
    (buffer[1], buffer[2]) = encode_u16(addr);
    buffer[3] = data[0];
    buffer[4] = data[1];
    5
}

// function code: 1 Byte 0x06
// register address: 2 Bytes 0x0000 to 0xFFFF
// register value: 2 Bytes
// Error
// function code: 1 Byte Function code + 0x80 , 0x86
// exception code: 1 Byte 01 | 02 | 03 | 04
// TODO
pub fn decode_write_single_register(buffer: &[u8]) -> Result<(), ModbusError> {
    if buffer[0] == 0x86 {
        // todo
    }

    if buffer[0] != 0x06 {
        // todo
    }

    Ok(())
}

// function code: 1 Byte 0x10
// starting address: 2 Bytes 0x0000-0xFFFF
// quantity of registers: 2 Bytes 0x0001-0x007B
// Byte count: 1 Byte  2 * N
// register value: 2 * N Bytes
pub fn encode_write_multiple_registers(buffer: &mut [u8], addr: u16, data: Vec<u8>) -> u16 {
    buffer[0] = 0x10;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(data.len() as u16);
    (buffer[5]) = data.len() as u8;

    for (i, v) in data.iter().enumerate() {
        buffer[5 + i] = *v;
    }
    5 + (data.len() as u16)
}

// function code: 1 Byte 0x10
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of registers: 1-123(0x7B)
// Error
// error code: 1 Byte 0x90
// exception code: 1 Byte  01 | 02 | 03 | 04
pub fn decode_write_multiple_registers(buffer: &[u8]) -> Result<(), ModbusError> {
    if buffer[0] != 0x90 {
        // todo
    }

    if buffer[0] != 0x10 {
        // todo
    }

    Ok(())
}

// function code: 1 Byte 0x16
// refrence address: 2 Bytes 0x0000 to 0xFFFF
// and_mask: 2 Bytes 0x0000 to 0xFFFF
// or_mask: 2 Bytes 0x0000 to 0xFFFF
pub fn encode_mask_write_register(buffer: &mut [u8], addr: u16) -> usize {
    buffer[0] = 0x10;
    (buffer[1], buffer[2]) = encode_u16(addr);

    7
}

// function code: 1 Byte 0x16
// reference address: 2 Bytes 0x0000-0xFFFF
// and_mask: 2 Bytes 0x0000-0xFFFF
// or_mask: 2 Bytes 0x0000-0xFFFF
// Error
// error code: 1 Byte 0x96
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn deocde_mask_write_register(buffer: &mut [u8]) {
    if buffer[0] != 0x96 {
        // todo
    }

    if buffer[0] != 0x16 {
        // todo
    }
}
