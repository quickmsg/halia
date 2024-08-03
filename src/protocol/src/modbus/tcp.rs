use std::{fmt::Debug, io};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt};

use super::{decode_u16, encode_u16, Context, FunctionCode, ModbusError, ProtocolError};

struct TcpContext<T> {
    transcation_id: u16,
    function_code: u8,
    slave: u8,
    buffer: [u8; 255],
    buffer_len: usize,
    transport: T,
}

pub fn new<T>(transport: T) -> io::Result<Box<dyn Context>>
where
    T: AsyncRead + AsyncWrite + Debug + Unpin + Send + 'static,
{
    Ok(Box::new(TcpContext {
        function_code: 0,
        transcation_id: 0,
        slave: 0,
        buffer: [0; 255],
        buffer_len: 0,
        transport,
    }))
}

impl<T> TcpContext<T> {
    fn update_transcation_id(&mut self) {
        if self.transcation_id < u16::MAX {
            self.transcation_id += 1;
        } else {
            self.transcation_id = 0;
        }
    }

    fn encode_common(&mut self, function_code: FunctionCode, slave: u8, addr: u16) {
        self.update_transcation_id();

        (self.buffer[0], self.buffer[1]) = encode_u16(self.transcation_id);

        self.buffer[2] = 0;
        self.buffer[3] = 0;

        self.buffer[6] = slave;
        self.slave = slave;

        let fc: u8 = function_code.into();
        self.buffer[7] = fc;
        self.function_code = fc;

        (self.buffer[8], self.buffer[9]) = encode_u16(addr);
    }

    // 12
    fn encode_read(&mut self, function_code: FunctionCode, slave: u8, addr: u16, quantity: u16) {
        self.encode_common(function_code, slave, addr);
        self.buffer[4] = 0;
        self.buffer[5] = 6;

        (self.buffer[10], self.buffer[11]) = encode_u16(quantity);

        self.buffer_len = 12;
    }

    fn decode_common(&mut self, n: usize) -> Result<(), ProtocolError> {
        if n == 0 {
            return Err(ProtocolError::EmptyResp);
        }

        let transcation_id = decode_u16(self.buffer[0], self.buffer[1]);
        if transcation_id != self.transcation_id {
            return Err(ProtocolError::TranscationIdMismatch);
        }

        if self.buffer[2] != 0 || self.buffer[3] != 0 {
            return Err(ProtocolError::ProtocolIdErr);
        }

        let slave = self.buffer[6];
        if self.slave != slave {
            return Err(ProtocolError::UnitIdMismatch);
        }

        let function_code = self.buffer[7];
        if function_code != self.function_code {
            return Err(ProtocolError::UnitIdMismatch);
        }

        Ok(())
    }

    fn decode_read(&mut self, n: usize) -> Result<(), ProtocolError> {
        self.decode_common(n)?;

        if n < 9 {
            return Err(ProtocolError::DataTooSmall);
        }

        // let len = decode_u16(self.buffer[4], self.buffer[5]);

        self.buffer_len = (9 + self.buffer[8]) as usize;
        Ok(())
    }

    fn encode_write(&mut self, function_code: FunctionCode, slave: u8, addr: u16, value: &[u8]) {
        self.encode_common(function_code.clone(), slave, addr);

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
                self.buffer_len = 12;
            }
            FunctionCode::WriteSingleRegister => {
                assert_eq!(value.len(), 2);
                self.buffer[10] = value[0];
                self.buffer[11] = value[1];

                self.buffer[4] = 0;
                self.buffer[5] = 6;

                self.buffer_len = 12;
            }
            FunctionCode::WriteMultipleRegisters => {
                assert!(value.len() > 2 && value.len() <= u8::MAX as usize);

                let quantity = (value.len() / 2) as u16;

                (self.buffer[10], self.buffer[11]) = encode_u16(quantity);

                self.buffer[12] = value.len() as u8;

                for (n, v) in value.iter().enumerate() {
                    self.buffer[13 + n] = *v;
                }

                self.buffer[4] = 0;
                self.buffer[5] = 7 + value.len() as u8;

                self.buffer_len = 13 + value.len()
            }
            FunctionCode::MaskWriteRegister => {
                assert_eq!(value.len(), 4);

                self.buffer[4] = 0;
                self.buffer[5] = 10;

                self.buffer[10] = value[0];
                self.buffer[11] = value[1];
                self.buffer[12] = value[2];
                self.buffer[13] = value[3];

                self.buffer_len = 14
            }
            _ => unreachable!(),
        }
    }

    fn decode_write(&mut self, n: usize) -> Result<(), ProtocolError> {
        self.decode_common(n)?;
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
        Ok(())
    }
}

#[async_trait]
impl<T> Context for TcpContext<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn read(
        &mut self,
        function_code: FunctionCode,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError> {
        self.encode_read(function_code, slave, addr, quantity);
        if let Err(e) = self
            .transport
            .write_all(&self.buffer[..self.buffer_len])
            .await
        {
            return Err(ModbusError::Transport(e));
        }

        match self.transport.read(&mut self.buffer).await {
            Ok(n) => match self.decode_read(n) {
                Ok(_) => Ok(&mut self.buffer[9..self.buffer_len]),
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
        self.encode_write(function_code, slave, addr, value);
        match self
            .transport
            .write(&mut self.buffer[..self.buffer_len])
            .await
        {
            Ok(n) => match self.decode_write(n) {
                Ok(_) => Ok(()),
                Err(e) => Err(ModbusError::Protocol(e)),
            },
            Err(e) => Err(ModbusError::Transport(e)),
        }
    }
}
