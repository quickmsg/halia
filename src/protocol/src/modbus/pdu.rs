use super::encode_u16;

// function code: 1 Byte 0x01
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of coilds: 2 Bytes 1 to 2000(0x7D0)
pub fn encode_read_coils(buffer: &mut [u8], addr: u16, quantity: u16) {
    buffer[0] = 0x01;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(quantity);
}

// function code: 1 Byte 0x01
// byte count: 1 Byte N
// coil status: n byte, n = N or N+1
// N = quantity / 8, 如果quantity = 0; N = N+1
// Error
// function code: 1 Byte Function code + 0x80
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_read_coils(buffer: &mut [u8]) {
    if buffer[0] == 0x81 {
        // todo
    }

    if buffer[0] != 0x01 {
        // todo
    }
}

// function code: 1 Byte 0x02
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of inputs: 2 Bytes 1 to 2000(0x7D0)
pub fn encode_read_discrete_inputs(buffer: &mut [u8], addr: u16, quantity: u16) {
    buffer[0] = 0x02;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(quantity);
}

// function code: 1 Byte 0x02
// byte count: 1 Byte N
// input status: N * 1 Byte
// Error
// function code: 1 Byte Function code + 0x80 , 0x82
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_read_discrete_inputs(buffer: &mut [u8]) {
    if buffer[0] == 0x82 {
        // todo
    }

    if buffer[0] != 0x02 {
        // todo
    }
}

// function code: 1 Byte 0x03
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of registers: 2 Bytes 1 to 125(0x7D)
pub fn encode_read_holding_registers(buffer: &mut [u8], addr: u16, quantity: u16) {
    buffer[0] = 0x01;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(quantity);
}

// function code: 1 Byte 0x03
// byte count: 2 * N
// input status: N * 2 Bytes
// Error
// function code: 1 Byte Function code + 0x80 , 0x83
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_read_holding_registers(buffer: &mut [u8], addr: u16, quantity: u16) {
    if buffer[0] == 0x83 {
        // todo
    }

    if buffer[0] != 0x03 {
        // todo
    }
}

// function code: 1 Byte 0x04
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of input registers: 2 Bytes 1 to 125(0x7D)
pub fn encode_read_input_registers(buffer: &mut [u8], addr: u16, quantity: u16) {
    buffer[0] = 0x04;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(quantity);
}

// function code: 1 Byte 0x04
// byte count: 2 * N
// input status: N * 2 Bytes
// Error
// function code: 1 Byte Function code + 0x80 , 0x84
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_read_input_registers(buffer: &mut [u8]) {
    if buffer[0] == 0x84 {
        // todo
    }

    if buffer[0] != 0x04 {
        // todo
    }
}

// function code: 1 Byte 0x05
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of input registers: 2 Bytes 0x0000 or 0xFF00
pub fn encode_write_single_coil(buffer: &mut [u8], addr: u16, quantity: u16) {
    buffer[0] = 0x05;
    (buffer[1], buffer[2]) = encode_u16(addr);
    (buffer[3], buffer[4]) = encode_u16(quantity);
}

// function code: 1 Byte 0x05
// output address: 2 Bytes 0x0000 to 0xFFFF
// output value: 2 Bytes
// Error
// function code: 1 Byte Function code + 0x80 , 0x85
// exception code: 1 Byte 01 | 02 | 03 | 04
pub fn decode_write_single_coil(buffer: &mut [u8]) {
    if buffer[0] == 0x85 {
        // todo
    }

    if buffer[0] != 0x05 {
        // todo
    }
}

// function code: 1 Byte 0x06
// starting address: 2 Bytes 0x0000 to 0xFFFF
// register value: 2 Bytes 0x0000 or 0xFFFF
pub fn encode_write_single_register(buffer: &mut [u8], addr: u16) {
    buffer[0] = 0x06;
    (buffer[1], buffer[2]) = encode_u16(addr);
    // (buffer[3], buffer[4]) = encode_u16(quantity);
}

// function code: 1 Byte 0x06
// register address: 2 Bytes 0x0000 to 0xFFFF
// register value: 2 Bytes
// Error
// function code: 1 Byte Function code + 0x80 , 0x86
// exception code: 1 Byte 01 | 02 | 03 | 04
// TODO
pub fn deocode_write_single_register(buffer: &mut [u8]) {
    if buffer[0] == 0x86 {
        // todo
    }

    if buffer[0] != 0x06 {
        // todo
    }
}

// function code: 1 Byte 0x10
// starting address: 2 Bytes 0x0000-0xFFFF
// quantity of registers: 2 Bytes 0x0001-0x007B
// Byte count: 1 Byte  2 * N
// register value: 2 * N Bytes
pub fn encode_write_multiple_registers(buffer: &mut [u8], addr: u16, quantity: u16) {
    buffer[0] = 0x10;
    (buffer[1], buffer[2]) = encode_u16(addr);
    // (buffer[3], buffer[4]) = encode_u16(quantity);
}

// function code: 1 Byte 0x10
// starting address: 2 Bytes 0x0000 to 0xFFFF
// quantity of registers: 1-123(0x7B)
// Error
// error code: 1 Byte 0x90
// exception code: 1 Byte  01 | 02 | 03 | 04
pub fn decode_write_multiple_registers(buffer: &mut [u8]) {
    if buffer[0] != 0x90 {
        // todo
    }

    if buffer[0] != 0x10 {
        // todo
    }
}

// function code: 1 Byte 0x16
// refrence address: 2 Bytes 0x0000 to 0xFFFF
// and_mask: 2 Bytes 0x0000 to 0xFFFF
// or_mask: 2 Bytes 0x0000 to 0xFFFF
pub fn encode_mask_write_register(buffer: &mut [u8], addr: u16) {
    buffer[0] = 0x10;
    (buffer[1], buffer[2]) = encode_u16(addr);
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
