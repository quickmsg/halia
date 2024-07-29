use std::{io, net::SocketAddr};

use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt},
    net::TcpStream,
};

use super::{Context, Error, FunctionCode, ProtocolError};

struct TcpContext {
    transcation_id: u16,
    function_code: FunctionCode,
    unit_id: u8,
    buffer: [u8; 255],
    stream: TcpStream,
}

impl TcpContext {
    fn update_transcation_id(&mut self) {
        if self.transcation_id < u16::MAX {
            self.transcation_id += 1;
        } else {
            self.transcation_id = 0;
        }
    }

    // 12
    fn encode_read(&mut self, function_code: FunctionCode, slave: u8, addr: u16, quantity: u16) {
        self.update_transcation_id();

        self.buffer[0] = (self.transcation_id >> 8) as u8;
        self.buffer[1] = (self.transcation_id & 0x00ff) as u8;

        self.buffer[2] = 0;
        self.buffer[3] = 0;

        self.buffer[4] = 0;
        self.buffer[5] = 6;

        self.buffer[6] = slave;
        self.unit_id = slave;

        self.function_code = function_code.clone();
        self.buffer[7] = function_code.into();

        self.buffer[8] = (addr >> 8) as u8;
        self.buffer[9] = (addr & 0x00ff) as u8;

        self.buffer[10] = (quantity >> 8) as u8;
        self.buffer[11] = (quantity & 0x00ff) as u8;
    }

    fn decode_read(&mut self, n: usize) -> Result<usize, ProtocolError> {
        if n == 0 {
            return Err(ProtocolError::EmptyResp);
        }
        if n < 9 {
            return Err(ProtocolError::DataTooSmall);
        }
        let transcation_id = ((self.buffer[0] as u16) << 8) | (self.buffer[1] as u16);
        if transcation_id != self.transcation_id {
            return Err(ProtocolError::TranscationIdMismatch);
        }

        if self.buffer[2] != 0 || self.buffer[3] != 0 {
            return Err(ProtocolError::ProtocolIdErr);
        }

        let len = ((self.buffer[4] as u16) << 8) | (self.buffer[5] as u16);

        let unit_id = self.buffer[6];
        if self.unit_id != unit_id {
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
        slave: u8,
        addr: u16,
        function_code: FunctionCode,
        value: &[u8],
    ) -> &[u8] {
        self.update_transcation_id();
        self.buffer[0] = (self.transcation_id >> 8) as u8;
        self.buffer[1] = (self.transcation_id & 0x00ff) as u8;

        self.buffer[2] = 0;
        self.buffer[3] = 0;

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
                &self.buffer[..12]
            }
            FunctionCode::WriteSingleRegister => {
                assert_eq!(value.len(), 2);
                self.buffer[10] = value[0];
                self.buffer[11] = value[1];

                self.buffer[4] = 0;
                self.buffer[5] = 6;

                &self.buffer[..12]
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

                &self.buffer[..13 + value.len()]
            }
            FunctionCode::MaskWriteRegister => {
                assert_eq!(value.len(), 4);

                self.buffer[4] = 0;
                self.buffer[5] = 10;

                self.buffer[10] = value[0];
                self.buffer[11] = value[1];
                self.buffer[12] = value[2];
                self.buffer[13] = value[3];

                &self.buffer[..14]
            }
            _ => unreachable!(),
        }
    }

    fn decode_write(&mut self) {

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
    }
}

pub async fn new(addr: SocketAddr) -> io::Result<impl Context> {
    let stream = TcpStream::connect(addr).await?;

    Ok(TcpContext {
        stream,
        function_code: FunctionCode::MaskWriteRegister,
        transcation_id: 0,
        unit_id: 0,
        buffer: [0; 255],
    })
}

impl Context for TcpContext {
    async fn read(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], Error> {
        self.encode_read(function_code, slave, addr, quantity);
        if let Err(e) = self.stream.write_all(&self.buffer[..12]).await {
            return Err(Error::Transport(e));
        }

        match self.stream.read(&mut self.buffer).await {
            Ok(n) => match self.decode_read(n) {
                Ok(n) => Ok(&mut self.buffer[9..9 + n]),
                Err(e) => Err(Error::Protocol(e)),
            },
            Err(e) => Err(Error::Transport(e)),
        }
    }

    async fn write(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        value: &[u8],
    ) -> Result<(), Error> {
        todo!()
    }
}
