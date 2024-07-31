use std::{io, net::SocketAddr};

use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt},
    net::TcpStream,
};

use super::{encode_u16, Context, FunctionCode, ModbusError, ProtocolError};

struct RtuContext {
    function_code: u8,
    slave: u8,
    buffer: [u8; 255],
    buffer_len: u8,
    stream: TcpStream,
}

pub async fn new(addr: SocketAddr) -> io::Result<impl Context> {
    let stream = TcpStream::connect(addr).await?;

    Ok(RtuContext {
        function_code: 0,
        stream,
        slave: 0,
        buffer: [0; 255],
        buffer_len: 0,
    })
}

impl RtuContext {
    fn encode_common(&mut self, function_code: FunctionCode, slave: u8, addr: u16) {
        self.buffer[0] = slave;
        self.slave = slave;

        let fc = function_code.into();
        self.function_code = fc;
        self.buffer[1] = fc;

        (self.buffer[2], self.buffer[3]) = encode_u16(addr);
    }

    // 12
    fn encode_read(&mut self, function_code: FunctionCode, slave: u8, addr: u16, quantity: u16) {
        self.encode_common(function_code, slave, addr);

        (self.buffer[10], self.buffer[11]) = encode_u16(quantity);
    }

    fn decode_read(&mut self, n: usize) -> Result<usize, ProtocolError> {
        if n == 0 {
            return Err(ProtocolError::EmptyResp);
        }
        if n < 9 {
            return Err(ProtocolError::DataTooSmall);
        }

        if self.buffer[2] != 0 || self.buffer[3] != 0 {
            return Err(ProtocolError::ProtocolIdErr);
        }

        let len = ((self.buffer[4] as u16) << 8) | (self.buffer[5] as u16);

        let slave = self.buffer[6];
        if self.slave != slave {
            return Err(ProtocolError::UnitIdMismatch);
        }

        let function_code = self.buffer[7];
        // if FunctionCode::from(function_code) != self.function_code {
        //     todo!()
        // }

        let byte_cnt = self.buffer[8];
        Ok(byte_cnt as usize)
        // Ok(&mut self.buffer[9..(9 + byte_cnt as usize)])
        //
    }

    fn encode_write(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        value: &[u8],
    ) -> usize {
        self.buffer[6] = slave;

        self.buffer[7] = function_code.clone().into();

        self.buffer[8] = (addr >> 8) as u8;
        self.buffer[9] = (addr & 0x00FF) as u8;

        match function_code {
            FunctionCode::WriteSingleCoil => {
                assert_eq!(value.len(), 1);
                match value[0] {
                    0 => {
                        self.buffer[10] = 0;
                        self.buffer[11] = 0;
                    }
                    1 => {
                        self.buffer[10] = 0xFF;
                        self.buffer[11] = 0;
                    }
                    _ => unreachable!(),
                }
                self.buffer[4] = 0;
                self.buffer[5] = 6;
                12
            }
            FunctionCode::WriteSingleRegister => {
                assert_eq!(value.len(), 2);
                self.buffer[10] = value[0];
                self.buffer[11] = value[1];

                self.buffer[4] = 0;
                self.buffer[5] = 6;

                12
            }
            FunctionCode::WriteMultipleRegisters => {
                assert!(value.len() > 2 && value.len() <= u8::MAX as usize);

                let quantity = (value.len() / 2) as u16;
                self.buffer[10] = (quantity >> 8) as u8;
                self.buffer[11] = (quantity & 0x00ff) as u8;
                self.buffer[12] = value.len() as u8;

                for (n, v) in value.iter().enumerate() {
                    self.buffer[13 + n] = *v;
                }

                self.buffer[4] = 0;
                self.buffer[5] = 7 + value.len() as u8;

                13 + value.len()
            }
            FunctionCode::MaskWriteRegister => {
                assert_eq!(value.len(), 4);

                self.buffer[4] = 0;
                self.buffer[5] = 10;

                self.buffer[10] = value[0];
                self.buffer[11] = value[1];
                self.buffer[12] = value[2];
                self.buffer[13] = value[3];

                14
            }
            _ => unreachable!(),
        }
    }

    fn decode_write(&mut self, n: usize) -> Result<(), ProtocolError> {
        // 1.	Transaction Identifier (2 bytes): 事务标识符，与请求一致。
        // 2.	Protocol Identifier (2 bytes): 协议标识符，与请求一致。
        // 3.	Length (2 bytes): 报文长度，表示从单元标识符到数据的总字节数。
        // 4.	Unit Identifier (1 byte): 单元标识符，与请求一致。
        // 5.	Function Code (1 byte): 功能码，0x05表示“写单个线圈”。
        // 6.	Output Address (2 bytes): 线圈地址，与请求一致。
        // 7.	Output Value (2 bytes): 线圈值，与请求一致。

        //     Transaction Identifier (2 bytes): 事务标识符，与请求一致。
        // 2.	Protocol Identifier (2 bytes): 协议标识符，与请求一致。
        // 3.	Length (2 bytes): 报文长度，表示从单元标识符到数据的总字节数。
        // 4.	Unit Identifier (1 byte): 单元标识符，与请求一致。
        // 5.	Function Code (1 byte): 功能码，0x06表示“写单个寄存器”。
        // 6.	Register Address (2 bytes): 寄存器地址，与请求一致。
        // 7.	Register Value (2 bytes): 寄存器值，与请求一致。

        //     1.	Transaction Identifier (2 bytes): 事务标识符，与请求一致。
        // 2.	Protocol Identifier (2 bytes): 协议标识符，与请求一致。
        // 3.	Length (2 bytes): 报文长度，表示从单元标识符到数据的总字节数。
        // 4.	Unit Identifier (1 byte): 单元标识符，与请求一致。
        // 5.	Function Code (1 byte): 功能码，0x10表示“写多个寄存器”。
        // 6.	Starting Address (2 bytes): 寄存器起始地址，与请求一致。
        // 7.	Quantity of Registers (2 bytes): 要写入的寄存器数量，与请求一致。

        //     1.	Transaction Identifier (2 bytes): 事务标识符，与请求一致。
        // 2.	Protocol Identifier (2 bytes): 协议标识符，与请求一致。
        // 3.	Length (2 bytes): 报文长度，表示从单元标识符到数据的总字节数。
        // 4.	Unit Identifier (1 byte): 单元标识符，与请求一致。
        // 5.	Function Code (1 byte): 功能码，0x16表示“掩码写寄存器”。
        // 6.	Reference Address (2 bytes): 寄存器地址，与请求一致。
        // 7.	And Mask (2 bytes): 与掩码，与请求一致。
        // 8.	Or Mask (2 bytes): 或掩码，与请求一致。
        todo!()
    }

    fn crc16(&self) -> (u8, u8) {
        let mut crc_hi = 0xFF;
        let mut crc_lo = 0xFF;
        for n in 0..(self.buffer_len as usize) {
            let index = (crc_hi ^ self.buffer[n]) as usize;
            crc_hi = crc_lo ^ table_crc_hi[index];
            crc_lo = table_crc_lo[index];
        }

        (crc_hi, crc_lo)
    }
}

impl Context for RtuContext {
    async fn read(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError> {
        self.encode_read(function_code, slave, addr, quantity);
        if let Err(e) = self.stream.write_all(&self.buffer[..12]).await {
            return Err(ModbusError::Transport(e));
        }

        match self.stream.read(&mut self.buffer).await {
            Ok(n) => match self.decode_read(n) {
                Ok(n) => Ok(&mut self.buffer[9..9 + n]),
                Err(e) => Err(ModbusError::Protocol(e)),
            },
            Err(e) => Err(ModbusError::Transport(e)),
        }
    }

    async fn write(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        value: &[u8],
    ) -> Result<(), ModbusError> {
        let len = self.encode_write(function_code, slave, addr, value);

        match self.stream.write(&mut self.buffer[..len]).await {
            Ok(n) => match self.decode_write(n) {
                Ok(_) => Ok(()),
                Err(e) => Err(ModbusError::Protocol(e)),
            },
            Err(e) => Err(ModbusError::Transport(e)),
        }
    }
}

const table_crc_hi: [u8; 256] = [
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
    0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
];

const table_crc_lo: [u8; 256] = [
    0x00, 0xC0, 0xC1, 0x01, 0xC3, 0x03, 0x02, 0xC2, 0xC6, 0x06, 0x07, 0xC7, 0x05, 0xC5, 0xC4, 0x04,
    0xCC, 0x0C, 0x0D, 0xCD, 0x0F, 0xCF, 0xCE, 0x0E, 0x0A, 0xCA, 0xCB, 0x0B, 0xC9, 0x09, 0x08, 0xC8,
    0xD8, 0x18, 0x19, 0xD9, 0x1B, 0xDB, 0xDA, 0x1A, 0x1E, 0xDE, 0xDF, 0x1F, 0xDD, 0x1D, 0x1C, 0xDC,
    0x14, 0xD4, 0xD5, 0x15, 0xD7, 0x17, 0x16, 0xD6, 0xD2, 0x12, 0x13, 0xD3, 0x11, 0xD1, 0xD0, 0x10,
    0xF0, 0x30, 0x31, 0xF1, 0x33, 0xF3, 0xF2, 0x32, 0x36, 0xF6, 0xF7, 0x37, 0xF5, 0x35, 0x34, 0xF4,
    0x3C, 0xFC, 0xFD, 0x3D, 0xFF, 0x3F, 0x3E, 0xFE, 0xFA, 0x3A, 0x3B, 0xFB, 0x39, 0xF9, 0xF8, 0x38,
    0x28, 0xE8, 0xE9, 0x29, 0xEB, 0x2B, 0x2A, 0xEA, 0xEE, 0x2E, 0x2F, 0xEF, 0x2D, 0xED, 0xEC, 0x2C,
    0xE4, 0x24, 0x25, 0xE5, 0x27, 0xE7, 0xE6, 0x26, 0x22, 0xE2, 0xE3, 0x23, 0xE1, 0x21, 0x20, 0xE0,
    0xA0, 0x60, 0x61, 0xA1, 0x63, 0xA3, 0xA2, 0x62, 0x66, 0xA6, 0xA7, 0x67, 0xA5, 0x65, 0x64, 0xA4,
    0x6C, 0xAC, 0xAD, 0x6D, 0xAF, 0x6F, 0x6E, 0xAE, 0xAA, 0x6A, 0x6B, 0xAB, 0x69, 0xA9, 0xA8, 0x68,
    0x78, 0xB8, 0xB9, 0x79, 0xBB, 0x7B, 0x7A, 0xBA, 0xBE, 0x7E, 0x7F, 0xBF, 0x7D, 0xBD, 0xBC, 0x7C,
    0xB4, 0x74, 0x75, 0xB5, 0x77, 0xB7, 0xB6, 0x76, 0x72, 0xB2, 0xB3, 0x73, 0xB1, 0x71, 0x70, 0xB0,
    0x50, 0x90, 0x91, 0x51, 0x93, 0x53, 0x52, 0x92, 0x96, 0x56, 0x57, 0x97, 0x55, 0x95, 0x94, 0x54,
    0x9C, 0x5C, 0x5D, 0x9D, 0x5F, 0x9F, 0x9E, 0x5E, 0x5A, 0x9A, 0x9B, 0x5B, 0x99, 0x59, 0x58, 0x98,
    0x88, 0x48, 0x49, 0x89, 0x4B, 0x8B, 0x8A, 0x4A, 0x4E, 0x8E, 0x8F, 0x4F, 0x8D, 0x4D, 0x4C, 0x8C,
    0x44, 0x84, 0x85, 0x45, 0x87, 0x47, 0x46, 0x86, 0x82, 0x42, 0x43, 0x83, 0x41, 0x81, 0x80, 0x40,
];
