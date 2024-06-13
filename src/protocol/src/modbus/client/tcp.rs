use tokio::io::{AsyncRead, AsyncWrite};

use crate::modbus::service;

use super::*;

pub fn attach<T>(transport: T) -> Context
where
    T: AsyncRead + AsyncWrite + Debug + Unpin + Send + 'static,
{
    let client = service::tcp::Client::new(transport, 0);
    Context {
        client: Box::new(client),
    }
}
