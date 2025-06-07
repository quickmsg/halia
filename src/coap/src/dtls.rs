use super::client::ClientTransport;
use super::server::{Responder, TransportRequestSender};
use async_trait::async_trait;
use std::net::SocketAddr;
use std::time::Duration;
use std::{
    io::{Error, ErrorKind, Result as IoResult},
    sync::Arc,
};
use tokio::net::UdpSocket;
use tokio::time::timeout;
use webrtc_dtls::conn::DTLSConn;
use webrtc_dtls::state::State;
use webrtc_util::conn::Conn;

pub struct DtlsResponse {
    pub conn: Arc<dyn Conn + Send + Sync>,
    pub remote_addr: SocketAddr,
}

#[async_trait]
impl ClientTransport for DtlsConnection {
    async fn recv(&self, buf: &mut [u8]) -> IoResult<(usize, Option<SocketAddr>)> {
        let read = self
            .conn
            .read(buf, None)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
        return Ok((read, self.conn.remote_addr()));
    }

    async fn send(&self, buf: &[u8]) -> IoResult<usize> {
        self.conn
            .write(buf, None)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e))
    }
}
#[async_trait]
impl Responder for DtlsResponse {
    /// responds to a request by creating a new task. This ensures we do not
    /// block the main server handler task
    async fn respond(&self, response: Vec<u8>) {
        let self_clone = self.conn.clone();
        tokio::spawn(async move { self_clone.send(&response).await });
    }
    fn address(&self) -> SocketAddr {
        self.remote_addr
    }
}

pub async fn spawn_webrtc_conn(
    conn: Arc<dyn Conn + Send + Sync>,
    remote_addr: SocketAddr,
    sender: TransportRequestSender,
) {
    const VECTOR_LENGTH: usize = 1600;
    loop {
        let mut vec_buf = Vec::with_capacity(VECTOR_LENGTH);
        unsafe { vec_buf.set_len(VECTOR_LENGTH) };
        let Ok(rx) = conn.recv(&mut vec_buf).await else {
            break;
        };
        if rx == 0 || rx > VECTOR_LENGTH {
            break;
        }
        unsafe { vec_buf.set_len(rx) }
        let response = Arc::new(DtlsResponse {
            conn: conn.clone(),
            remote_addr,
        });
        let Ok(_) = sender.send((vec_buf, response)) else {
            break;
        };
    }
}
#[async_trait]
/// This trait is used to implement a hook that is called when a DTLS connection is dropped
/// Only use this in case you need to save your connection
pub trait DtlsDropHook: Send + Sync {
    async fn on_drop(&self, conn: Arc<DTLSConn>);
}
pub struct DtlsConnection {
    conn: Arc<DTLSConn>,
    on_drop: Option<Box<dyn DtlsDropHook>>,
}

impl DtlsConnection {
    /// Creates a new DTLS connection from a given connection. This connection can be
    /// a tokio UDP socket or a user-created struct implementing Conn, Send, and Sync
    ///
    ///
    /// # Errors
    ///
    /// This function will return an error if the handshake fails or if it times out
    pub async fn try_from_connection(
        connection: Arc<dyn Conn + Send + Sync>,
        dtls_config: webrtc_dtls::config::Config,
        handshake_timeout: Duration,
        state: Option<State>,
        on_drop: Option<Box<dyn DtlsDropHook>>,
    ) -> IoResult<Self> {
        let dtls_conn = timeout(
            handshake_timeout,
            DTLSConn::new(connection, dtls_config, true, state),
        )
        .await
        .map_err(|_| {
            Error::new(
                ErrorKind::TimedOut,
                "Received no response on DTLS handshake",
            )
        })?
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
        return Ok(DtlsConnection {
            conn: Arc::new(dtls_conn),
            on_drop,
        });
    }

    pub async fn try_new(dtls_config: UdpDtlsConfig) -> IoResult<DtlsConnection> {
        let conn = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
        conn.connect(dtls_config.dest_addr).await?;
        return Self::try_from_connection(
            Arc::new(conn),
            dtls_config.config,
            Duration::new(30, 0),
            None,
            None,
        )
        .await;
    }
}
pub struct UdpDtlsConfig {
    pub config: webrtc_dtls::config::Config,
    pub dest_addr: SocketAddr,
}

impl Drop for DtlsConnection {
    fn drop(&mut self) {
        if let Some(drop_hook) = self.on_drop.take() {
            println!("dropping");
            //this is a nasty hack necessary to call async methods inside the drop method without
            //transferring ownership
            let conn = self.conn.clone();
            tokio::spawn(async move {
                drop_hook.on_drop(conn).await;
            });
        }
    }
}