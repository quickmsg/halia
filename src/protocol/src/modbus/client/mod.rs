use std::{borrow::Cow, fmt::Debug, io};

use async_trait::async_trait;

use super::{Error, Request, Response, Result, SlaveContext};

pub mod rtu;
pub mod tcp;

#[async_trait]
pub trait Client: SlaveContext + Send + Debug {
    async fn call(&mut self, request: Request<'_>) -> Result<Response>;
}

#[async_trait]
pub trait Reader: Client {
    /// Read multiple coils (0x01)
    async fn read_coils(&mut self, addr: u16, cnt: u16) -> Result<Vec<u8>>;

    /// Read multiple discrete inputs (0x02)
    async fn read_discrete_inputs(&mut self, addr: u16, cnt: u16) -> Result<Vec<u8>>;

    /// Read multiple holding registers (0x03)
    async fn read_holding_registers(&mut self, addr: u16, cnt: u16) -> Result<Vec<u8>>;

    /// Read multiple input registers (0x04)
    async fn read_input_registers(&mut self, addr: u16, cnt: u16) -> Result<Vec<u8>>;
}

/// Asynchronous Modbus writer
#[async_trait]
pub trait Writer: Client {
    /// Write a single u8 (0x05)
    async fn write_single_coil(&mut self, addr: u16, coil: u8) -> Result<()>;

    /// Write a single holding register (0x06)
    async fn write_single_register(&mut self, addr: u16, register: &'_ [u8]) -> Result<()>;

    /// Write multiple u8s (0x0F)
    async fn write_multiple_coils(&mut self, addr: u16, coils: &'_ [u8]) -> Result<()>;

    /// Write multiple holding registers (0x10)
    async fn write_multiple_registers(&mut self, addr: u16, register: &'_ [u8]) -> Result<()>;
}

/// Asynchronous Modbus client context
#[derive(Debug)]
pub struct Context {
    client: Box<dyn Client>,
}

impl Context {
    /// Disconnect the client
    pub async fn disconnect(&mut self) -> Result<()> {
        // Disconnecting is expected to fail!
        let res = self.client.call(Request::Disconnect).await;
        match res {
            Ok(_) => unreachable!(),
            Err(Error::Transport(err)) => match err.kind() {
                io::ErrorKind::NotConnected | io::ErrorKind::BrokenPipe => Ok(Ok(())),
                _ => Err(Error::Transport(err)),
            },
            Err(err) => Err(err),
        }
    }
}

impl From<Box<dyn Client>> for Context {
    fn from(client: Box<dyn Client>) -> Self {
        Self { client }
    }
}

impl From<Context> for Box<dyn Client> {
    fn from(val: Context) -> Self {
        val.client
    }
}

#[async_trait]
impl Client for Context {
    async fn call(&mut self, request: Request<'_>) -> Result<Response> {
        self.client.call(request).await
    }
}

impl SlaveContext for Context {
    fn set_slave(&mut self, slave: u8) {
        self.client.set_slave(slave);
    }
}

#[async_trait]
impl Reader for Context {
    async fn read_coils<'a>(&'a mut self, addr: u16, cnt: u16) -> Result<Vec<u8>> {
        self.client
            .call(Request::ReadCoils(addr, cnt))
            .await
            .map(|result| {
                result.map_err(Into::into).map(|response| match response {
                    Response::ReadCoils(coils) => coils,
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
    }

    async fn read_discrete_inputs<'a>(&'a mut self, addr: u16, cnt: u16) -> Result<Vec<u8>> {
        self.client
            .call(Request::ReadDiscreteInputs(addr, cnt))
            .await
            .map(|result| {
                result.map_err(Into::into).map(|response| match response {
                    Response::ReadDiscreteInputs(coils) => coils,
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
    }

    async fn read_input_registers<'a>(&'a mut self, addr: u16, cnt: u16) -> Result<Vec<u8>> {
        self.client
            .call(Request::ReadInputRegisters(addr, cnt))
            .await
            .map(|result| {
                result.map_err(Into::into).map(|response| match response {
                    Response::ReadInputRegisters(data) => data,
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
    }

    async fn read_holding_registers<'a>(&'a mut self, addr: u16, cnt: u16) -> Result<Vec<u8>> {
        self.client
            .call(Request::ReadHoldingRegisters(addr, cnt))
            .await
            .map(|result| {
                result.map_err(Into::into).map(|response| match response {
                    Response::ReadHoldingRegisters(data) => data,
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
    }
}

#[async_trait]
impl Writer for Context {
    async fn write_single_coil<'a>(&'a mut self, addr: u16, coil: u8) -> Result<()> {
        self.client
            .call(Request::WriteSingleCoil(addr, coil))
            .await
            .map(|result| result.map_err(Into::into).map(|_| {}))
    }

    async fn write_multiple_coils<'a>(&'a mut self, addr: u16, coils: &'_ [u8]) -> Result<()> {
        self.client
            .call(Request::WriteMultipleCoils(addr, Cow::Borrowed(coils)))
            .await
            .map(|result| result.map_err(Into::into).map(|_| {}))
    }

    async fn write_single_register<'a>(&'a mut self, addr: u16, register: &'_ [u8]) -> Result<()> {
        self.client
            .call(Request::WriteSingleRegister(addr, Cow::Borrowed(register)))
            .await
            .map(|result| result.map_err(Into::into).map(|_| {}))
    }

    async fn write_multiple_registers<'a>(
        &'a mut self,
        addr: u16,
        registers: &'_ [u8],
    ) -> Result<()> {
        self.client
            .call(Request::WriteMultipleRegisters(
                addr,
                Cow::Borrowed(registers),
            ))
            .await
            .map(|result| result.map_err(Into::into).map(|_| {}))
    }
}
