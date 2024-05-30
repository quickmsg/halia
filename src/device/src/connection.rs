use anyhow::Result;
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_serial::{SerialPortBuilderExt, SerialStream, StopBits};

use bytes::BytesMut;

pub enum Connection {
    TcpClient(TcpClientConnection),
    Serial(SerialConnection),
}

impl Connection {
    pub(crate) async fn connect(&mut self) -> Result<()> {
        match self {
            Connection::TcpClient(tcp) => tcp.connect().await,
            Connection::Serial(serial) => serial.connect().await,
        }
    }

    pub(crate) async fn send(&mut self, data: &mut BytesMut) -> Result<()> {
        match self {
            Connection::TcpClient(tcp) => tcp.send(data).await,
            Connection::Serial(serial) => serial.send(data).await,
        }
    }

    pub(crate) async fn recv(&mut self, data: &mut BytesMut) -> Result<()> {
        match self {
            Connection::TcpClient(tcp) => tcp.recv(data).await,
            Connection::Serial(serial) => serial.recv(data).await,
        }
    }
}

pub(crate) struct TcpClientConnection {
    ip: String,
    port: u16,
    tcp_stream: TcpStream,
    connected: bool,
}

impl TcpClientConnection {
    pub(crate) async fn new(ip: String, port: u16) -> Result<Connection> {
        let socket_addr: SocketAddr = format!("{}:{}", ip, port).parse()?;
        let tcp_stream = TcpStream::connect(socket_addr).await?;
        Ok(Connection::TcpClient(Self {
            ip,
            port,
            tcp_stream,
            connected: true,
        }))
    }

    pub(crate) async fn connect(&mut self) -> Result<()> {
        let socket_addr: SocketAddr = format!("{}:{}", self.ip, self.port).parse()?;
        match TcpStream::connect(socket_addr).await {
            Ok(tcp_stream) => {
                self.tcp_stream = tcp_stream;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn send(&mut self, data: &mut BytesMut) -> Result<()> {
        self.tcp_stream.write_buf(data).await?;
        self.tcp_stream.flush().await?;
        Ok(())
    }

    pub(crate) async fn recv(&mut self, data: &mut BytesMut) -> Result<()> {
        self.tcp_stream.read_buf(data).await?;
        Ok(())
    }
}

pub(crate) struct TcpServerConnection {}

pub(crate) struct SerialConnection {
    path: String,
    stop_bits: u8,
    baund_rate: u32,
    data_bits: u8,
    parity: u8,
    serial_stream: SerialStream,
}

impl SerialConnection {
    pub async fn new(
        path: String,
        stop_bits: u8,
        baund_rate: u32,
        data_bits: u8,
        parity: u8,
    ) -> Result<Connection> {
        let serial_stream = tokio_serial::new(&path, baund_rate)
            .stop_bits(StopBits::One)
            .data_bits(tokio_serial::DataBits::Eight)
            .parity(tokio_serial::Parity::Even)
            .open_native_async()?;

        Ok(Connection::Serial(Self {
            path: path,
            stop_bits: stop_bits,
            baund_rate: baund_rate,
            data_bits: data_bits,
            parity: parity,
            serial_stream,
        }))
    }

    pub(crate) async fn connect(&mut self) -> Result<()> {
        todo!()
    }

    pub(crate) async fn send(&mut self, data: &mut BytesMut) -> Result<()> {
        self.serial_stream.write_buf(data).await?;
        self.serial_stream.flush().await?;
        Ok(())
    }

    pub(crate) async fn recv(&mut self, data: &mut BytesMut) -> Result<()> {
        self.serial_stream.read_buf(data).await?;
        Ok(())
    }
}
