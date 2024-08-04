use std::{fmt::Debug, io};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::modbus::pdu::decode_read_coils;

use super::{
    decode_u16, encode_u16,
    pdu::{
        decode_read_discrete_inputs, decode_read_holding_registers, decode_read_input_registers,
        decode_write_multiple_registers, decode_write_single_coil, decode_write_single_register,
        encode_read_coils, encode_read_discrete_inputs, encode_read_holding_registers,
        encode_read_input_registers, encode_write_multiple_registers, encode_write_single_coil,
        encode_write_single_register,
    },
    Context, ModbusError, ProtocolError,
};

// 最大为260Bytes，MBAP为7Byte
struct TcpContext<T> {
    transcation_id: u16,
    slave: u8,
    buffer: [u8; 260],
    buffer_len: usize,
    transport: T,
}

pub fn new<T>(transport: T) -> io::Result<Box<dyn Context>>
where
    T: AsyncReadExt + AsyncWriteExt + Debug + Unpin + Send + 'static,
{
    Ok(Box::new(TcpContext {
        transcation_id: 0,
        slave: 0,
        buffer: [0; 260],
        buffer_len: 0,
        transport,
    }))
}

impl<T> TcpContext<T>
where
    T: AsyncReadExt + AsyncWriteExt + Unpin + Send,
{
    fn update_transcation_id(&mut self) {
        if self.transcation_id < u16::MAX {
            self.transcation_id += 1;
        } else {
            self.transcation_id = 0;
        }
    }

    fn encode_adu(&mut self, len: u16, slave: u8) {
        if self.transcation_id < u16::MAX {
            self.transcation_id += 1;
        } else {
            self.transcation_id = 0;
        }

        (self.buffer[0], self.buffer[1]) = encode_u16(self.transcation_id);

        // tcp 协议 固定为0
        self.buffer[2] = 0;
        self.buffer[3] = 0;

        (self.buffer[4], self.buffer[5]) = encode_u16(len + 1);

        self.slave = slave;
        self.buffer[6] = slave;

        self.buffer_len = 7 + (len as usize)
    }

    fn decode_adu(&mut self) -> Result<(), ProtocolError> {
        if self.buffer_len == 0 {
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

        Ok(())
    }

    async fn transport_read_send(&mut self) -> Result<(), ModbusError> {
        if let Err(e) = self
            .transport
            .write_all(&self.buffer[..self.buffer_len])
            .await
        {
            return Err(ModbusError::Transport(e));
        }

        match self.transport.read(&mut self.buffer).await {
            Ok(n) => self.buffer_len = n,
            Err(e) => return Err(ModbusError::Transport(e)),
        }

        Ok(())
    }
}

#[async_trait]
impl<T> Context for TcpContext<T>
where
    T: AsyncReadExt + AsyncWriteExt + Unpin + Send,
{
    async fn read_coils(
        &mut self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError> {
        let len = encode_read_coils(&mut self.buffer[7..], addr, quantity);
        self.encode_adu(len, slave);
        self.transport_read_send().await?;
        self.decode_adu()?;
        decode_read_coils(&mut self.buffer[7..])
    }

    async fn read_discrete_inputs(
        &mut self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError> {
        let len = encode_read_discrete_inputs(&mut self.buffer[7..], addr, quantity);
        self.encode_adu(len, slave);
        self.transport_read_send().await?;
        self.decode_adu()?;
        decode_read_discrete_inputs(&mut self.buffer[7..])
    }

    async fn read_holding_registers(
        &mut self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError> {
        let len = encode_read_holding_registers(&mut self.buffer[7..], addr, quantity);
        self.encode_adu(len, slave);
        self.transport_read_send().await?;
        self.decode_adu()?;
        decode_read_holding_registers(&mut self.buffer[7..])
    }

    async fn read_input_registers(
        &mut self,
        slave: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<&mut [u8], ModbusError> {
        let len = encode_read_input_registers(&mut self.buffer[7..], addr, quantity);
        self.encode_adu(len, slave);
        self.transport_read_send().await?;
        self.decode_adu()?;
        decode_read_input_registers(&mut self.buffer[7..])
    }

    async fn write_single_coil(
        &mut self,
        slave: u8,
        addr: u16,
        value: Vec<u8>,
    ) -> Result<(), ModbusError> {
        let len = encode_write_single_coil(&mut self.buffer[7..], addr, value);
        self.encode_adu(len, slave);
        self.transport_read_send().await?;
        self.decode_adu()?;
        decode_write_single_coil(&self.buffer[7..])
    }

    async fn write_single_register(
        &mut self,
        slave: u8,
        addr: u16,
        value: Vec<u8>,
    ) -> Result<(), ModbusError> {
        let len = encode_write_single_register(&mut self.buffer[7..], addr, value);
        self.encode_adu(len, slave);
        self.transport_read_send().await?;
        self.decode_adu()?;
        decode_write_single_register(&self.buffer[7..])
    }

    async fn write_multiple_registers(
        &mut self,
        slave: u8,
        addr: u16,
        value: Vec<u8>,
    ) -> Result<(), ModbusError> {
        let len = encode_write_multiple_registers(&mut self.buffer[7..], addr, value);
        self.encode_adu(len, slave);
        self.transport_read_send().await?;
        self.decode_adu()?;
        decode_write_multiple_registers(&self.buffer[7..])
    }

    async fn mask_write_register(
        &mut self,
        slave: u8,
        addr: u16,
        value: Vec<u8>,
    ) -> Result<(), ModbusError> {
        let len = encode_write_single_register(&mut self.buffer[7..], addr, value);
        self.encode_adu(len, slave);
        self.transport_read_send().await?;
        self.decode_adu()?;
        decode_write_single_register(&self.buffer[7..])
    }
}
