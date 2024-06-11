use tokio::io::{AsyncRead, AsyncWrite};

use crate::modbus::protocol::service;

use super::*;

pub(crate) fn attach<T>(transport: T) -> Context
where
    T: AsyncRead + AsyncWrite + Debug + Unpin + Send + 'static,
{
    let client = service::rtu::Client::new(transport, 0);
    Context {
        client: Box::new(client),
    }
}
