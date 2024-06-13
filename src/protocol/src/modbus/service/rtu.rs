use futures_util::{SinkExt as _, StreamExt as _};
use std::{fmt, io};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::modbus::{
    codec,
    frame::{
        rtu::{Header, RequestAdu},
        RequestPdu, ResponsePdu,
    },
    ExceptionResponse, ProtocolError, Request, Response, Result, SlaveContext,
};

use super::verify_response_header;

/// Modbus RTU client
#[derive(Debug)]
pub(crate) struct Client<T> {
    framed: Framed<T, codec::rtu::ClientCodec>,
    slave_id: u8,
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(transport: T, slave: u8) -> Self {
        let framed = Framed::new(transport, codec::rtu::ClientCodec::default());
        let slave_id = slave.into();
        Self { framed, slave_id }
    }

    fn next_request_adu<'a, R>(&self, req: R, disconnect: bool) -> RequestAdu<'a>
    where
        R: Into<RequestPdu<'a>>,
    {
        let slave_id = self.slave_id;
        let hdr = Header { slave_id };
        let pdu = req.into();
        RequestAdu {
            hdr,
            pdu,
            disconnect,
        }
    }

    async fn call(&mut self, req: Request<'_>) -> Result<Response> {
        let disconnect = req == Request::Disconnect;
        let req_function_code = req.function_code();
        let req_adu = self.next_request_adu(req, disconnect);
        let req_hdr = req_adu.hdr;

        self.framed.read_buffer_mut().clear();

        self.framed.send(req_adu).await?;
        let res_adu = self
            .framed
            .next()
            .await
            .unwrap_or_else(|| Err(io::Error::from(io::ErrorKind::BrokenPipe)))?;

        let ResponsePdu(result) = res_adu.pdu;

        // Match headers of request and response.
        if let Err(message) = verify_response_header(&req_hdr, &res_adu.hdr) {
            return Err(ProtocolError::HeaderMismatch { message, result }.into());
        }

        // Match function codes of request and response.
        let rsp_function_code = match &result {
            Ok(response) => response.function_code(),
            Err(ExceptionResponse { function, .. }) => *function,
        };
        if req_function_code != rsp_function_code {
            return Err(ProtocolError::FunctionCodeMismatch {
                request: req_function_code,
                result,
            }
            .into());
        }

        Ok(result.map_err(
            |ExceptionResponse {
                 function: _,
                 exception,
             }| exception,
        ))
    }
}

impl<T> SlaveContext for Client<T> {
    fn set_slave(&mut self, slave: u8) {
        self.slave_id = slave.into();
    }
}

#[async_trait::async_trait]
impl<T> crate::modbus::client::Client for Client<T>
where
    T: fmt::Debug + AsyncRead + AsyncWrite + Send + Unpin,
{
    async fn call(&mut self, req: Request<'_>) -> Result<Response> {
        self.call(req).await
    }
}