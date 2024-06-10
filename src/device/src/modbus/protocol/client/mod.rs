use std::{borrow::Cow, fmt::Debug, io};

use async_trait::async_trait;

use super::{slave::SlaveContext, Error, Request, Response, Result, Slave};

pub(crate) mod rtu;
pub(crate) mod tcp;

#[async_trait]
pub trait Client: SlaveContext + Send + Debug {
    async fn call(&mut self, request: Request<'_>) -> Result<Response>;
}

#[async_trait]
pub(crate) trait Reader: Client {
    async fn read_coils(&mut self, addr: u16, cnt: u16) -> Result<Vec<u8>>;

    /// Read multiple discrete inputs (0x02)
    async fn read_discrete_inputs(&mut self, addr: u16, cnt: u16) -> Result<Vec<u8>>;

    /// Read multiple holding registers (0x03)
    async fn read_holding_registers(&mut self, addr: u16, cnt: u16) -> Result<Vec<u8>>;

    /// Read multiple input registers (0x04)
    async fn read_input_registers(&mut self, addr: u16, cnt: u16) -> Result<Vec<u8>>;

    /// Read and write multiple holding registers (0x17)
    ///
    /// The write operation is performed before the read unlike
    /// the name of the operation might suggest!
    async fn read_write_multiple_registers(
        &mut self,
        read_addr: u16,
        read_count: u16,
        write_addr: u16,
        write_data: &[u8],
    ) -> Result<Vec<u8>>;
}

/// Asynchronous Modbus writer
#[async_trait]
pub trait Writer: Client {
    /// Write a single u8 (0x05)
    async fn write_single_Coil(&mut self, addr: u16, coil: &'_ [u8]) -> Result<()>;

    /// Write a single holding register (0x06)
    async fn write_single_register(&mut self, addr: u16, register: &'_ [u8]) -> Result<()>;

    /// Write multiple u8s (0x0F)
    async fn write_multiple_Coils(&mut self, addr: u16, coils: &'_ [u8]) -> Result<()>;

    /// Write multiple holding registers (0x10)
    async fn write_multiple_registers(&mut self, addr: u16, register: &'_ [u8]) -> Result<()>;

    // Set or clear individual bits of a holding register (0x16)
    // async fn masked_write_register(&mut self, addr: u16, and_mask: u8, or_mask: u8) -> Result<()>;
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
    fn set_slave(&mut self, slave: Slave) {
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
                    Response::ReadCoils(mut coils) => {
                        debug_assert!(coils.len() >= cnt.into());
                        coils.truncate(cnt.into());
                        coils
                    }
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
                    Response::ReadDiscreteInputs(mut u8s) => {
                        debug_assert!(u8s.len() >= cnt.into());
                        u8s.truncate(cnt.into());
                        u8s
                    }
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
                    Response::ReadInputRegisters(u8s) => u8s,
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
                    Response::ReadHoldingRegisters(u8s) => u8s,
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
    }

    async fn read_write_multiple_registers<'a>(
        &'a mut self,
        read_addr: u16,
        read_count: u16,
        write_addr: u16,
        write_data: &[u8],
    ) -> Result<Vec<u8>> {
        self.client
            .call(Request::ReadWriteMultipleRegisters(
                read_addr,
                read_count,
                write_addr,
                Cow::Borrowed(write_data),
            ))
            .await
            .map(|result| {
                result.map_err(Into::into).map(|response| match response {
                    Response::ReadWriteMultipleRegisters(u8s) => u8s,
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
    }
}

#[async_trait]
impl Writer for Context {
    async fn write_single_Coil<'a>(&'a mut self, addr: u16, coil: &'_ [u8]) -> Result<()> {
        self.client
            .call(Request::WriteSingleCoil(addr, Cow::Borrowed(coil)))
            .await
            .map(|result| {
                result.map_err(Into::into).map(|response| match response {
                    Response::WriteSingleCoil(rsp_addr, rsp_u8) => {}
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
    }

    async fn write_multiple_Coils<'a>(&'a mut self, addr: u16, coils: &'_ [u8]) -> Result<()> {
        self.client
            .call(Request::WriteMultipleCoils(addr, Cow::Borrowed(coils)))
            .await
            .map(|result| {
                result.map_err(Into::into).map(|response| match response {
                    Response::WriteMultipleCoils(rsp_addr, rsp_cnt) => {}
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
    }

    async fn write_single_register<'a>(&'a mut self, addr: u16, register: &'_ [u8]) -> Result<()> {
        self.client
            .call(Request::WriteSingleRegister(addr, Cow::Borrowed(register)))
            .await
            .map(|result| {
                result.map_err(Into::into).map(|response| match response {
                    Response::WriteSingleRegister(rsp_addr, rsp_u8) => {}
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
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
            .map(|result| {
                result.map_err(Into::into).map(|response| match response {
                    Response::WriteMultipleRegisters(rsp_addr, rsp_cnt) => {}
                    _ => unreachable!("call() should reject mismatching responses"),
                })
            })
    }

    // async fn masked_write_register<'a>(
    //     &'a mut self,
    //     addr: u16,
    //     and_mask: u8,
    //     or_mask: u8,
    // ) -> Result<()> {
    //     self.client
    //         .call(Request::MaskWriteRegister(addr, and_mask, or_mask))
    //         .await
    //         .map(|result| {
    //             result.map_err(Into::into).map(|response| match response {
    //                 Response::MaskWriteRegister(rsp_addr, rsp_and_mask, rsp_or_mask) => {
    //                     debug_assert_eq!(addr, rsp_addr);
    //                     debug_assert_eq!(and_mask, rsp_and_mask);
    //                     debug_assert_eq!(or_mask, rsp_or_mask);
    //                 }
    //                 _ => unreachable!("call() should reject mismatching responses"),
    //             })
    //         })
    // }
}

#[cfg(test)]
mod tests {
    use crate::Result;

    use super::*;
    use std::sync::Mutex;

    #[derive(Default, Debug)]
    pub(crate) struct ClientMock {
        slave: Option<Slave>,
        last_request: Mutex<Option<Request<'static>>>,
        next_response: Option<Result<Response>>,
    }

    #[allow(dead_code)]
    impl ClientMock {
        pub(crate) fn slave(&self) -> Option<Slave> {
            self.slave
        }

        pub(crate) fn last_request(&self) -> &Mutex<Option<Request<'static>>> {
            &self.last_request
        }

        pub(crate) fn set_next_response(&mut self, next_response: Result<Response>) {
            self.next_response = Some(next_response);
        }
    }

    // #[async_trait]
    // impl Client for ClientMock {
    //     async fn call(&mut self, request: Request<'_>) -> Result<Response> {
    //         *self.last_request.lock().unwrap() = Some(request.into_owned());
    //         match self.next_response.take().unwrap() {
    //             Ok(response) => Ok(response),
    //             Err(Error::Transport(err)) => {
    //                 Err(io::Error::new(err.kind(), format!("{err}")).into())
    //             }
    //             Err(err) => Err(err),
    //         }
    //     }
    // }

    impl SlaveContext for ClientMock {
        fn set_slave(&mut self, slave: Slave) {
            self.slave = Some(slave);
        }
    }

    // #[test]
    // fn read_some_u8s() {
    //     // The protocol will always return entire bytes with, i.e.
    //     // a multiple of 8 u8s.
    //     let response_u8s = [true, false, false, true, false, true, false, true];
    //     for num_u8s in 1..8 {
    //         let mut client = Box::<ClientMock>::default();
    //         client.set_next_response(Ok(Ok(Response::Readu8s(response_u8s.to_vec()))));
    //         let mut context = Context { client };
    //         context.set_slave(Slave(1));
    //         let u8s = futures::executor::block_on(context.read_u8s(1, num_u8s))
    //             .unwrap()
    //             .unwrap();
    //         assert_eq!(&response_u8s[0..num_u8s as usize], &u8s[..]);
    //     }
    // }

    // #[test]
    // fn read_some_discrete_inputs() {
    //     // The protocol will always return entire bytes with, i.e.
    //     // a multiple of 8 u8s.
    //     let response_inputs = [true, false, false, true, false, true, false, true];
    //     for num_inputs in 1..8 {
    //         let mut client = Box::<ClientMock>::default();
    //         client.set_next_response(Ok(Ok(Response::ReadDiscreteInputs(
    //             response_inputs.to_vec(),
    //         ))));
    //         let mut context = Context { client };
    //         context.set_slave(Slave(1));
    //         let inputs = futures::executor::block_on(context.read_discrete_inputs(1, num_inputs))
    //             .unwrap()
    //             .unwrap();
    //         assert_eq!(&response_inputs[0..num_inputs as usize], &inputs[..]);
    //     }
    // }
}
