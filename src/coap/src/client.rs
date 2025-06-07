use super::dtls::{DtlsConnection, UdpDtlsConfig};
use super::request::RequestBuilder;
use coap_lite::{
    block_handler::{extending_splice, BlockValue},
    error::HandlingError,
    CoapOption, CoapRequest, CoapResponse, MessageClass, MessageType, ObserveOption,
    Packet as Message, RequestType as Method, ResponseType as Status,
};
use std::mem;
use tracing::{debug, error, info, trace, warn};

use futures::Future;

use regex::Regex;
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{atomic::AtomicU16, Weak},
};
use std::{
    io::{Error, ErrorKind, Result as IoResult},
    pin::Pin,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot, Mutex,
};
use tokio::time::timeout;
use tokio::{
    net::{lookup_host, ToSocketAddrs, UdpSocket},
    sync::RwLock,
};
use url::Url;
const DEFAULT_RECEIVE_TIMEOUT_SECONDS: u64 = 2; // 2s

#[derive(Debug, Clone)]
pub struct Packet {
    pub address: Option<SocketAddr>,
    pub message: Message,
}

#[derive(Debug)]
pub enum ObserveMessage {
    Terminate,
}
use async_trait::async_trait;

#[async_trait]
/// A basic interface for a transport on the client
/// representing a one-to-one connection between a client and server
/// timeouts and retries do not need to be implemented by the transport
/// if confirmable messages are sent
pub trait ClientTransport: Send + Sync {
    async fn recv(&self, buf: &mut [u8]) -> std::io::Result<(usize, Option<SocketAddr>)>;
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize>;
}

trait TransportExt {
    async fn receive_packet(&self) -> IoResult<Option<Packet>>;
}

impl<T: ClientTransport> TransportExt for T {
    async fn receive_packet(&self) -> IoResult<Option<Packet>> {
        let mut buf = [0; 1500];
        let (nread, address) = self.recv(&mut buf).await?;
        return match Message::from_bytes(&buf[..nread]).ok() {
            Some(message) => Ok(Some(Packet { address, message })),
            None => Ok(None),
        };
    }
}

/// we only use the token as the identifier, and an empty token to represent empty requests
type Token = Vec<u8>;
type PacketRegistry = BTreeMap<Token, UnboundedSender<IoResult<Packet>>>;

#[derive(Clone)]
pub struct TransportSynchronizer {
    pub(crate) outgoing: Arc<Mutex<PacketRegistry>>,
    fail_error: Arc<RwLock<Option<std::io::Error>>>,
}

impl TransportSynchronizer {
    pub fn new() -> Self {
        Self {
            outgoing: Arc::new(Mutex::new(PacketRegistry::new())),
            fail_error: Arc::new(RwLock::new(None)),
        }
    }

    async fn check_for_error(&self, sender: &UnboundedSender<IoResult<Packet>>) -> Option<()> {
        self.fail_error.read().await.as_ref();
        if let Some(err) = self.fail_error.read().await.as_ref() {
            let _ = sender.send(Err(Self::clone_err(err)));
            return None;
        }
        Some(())
    }

    fn clone_err(error: &std::io::Error) -> std::io::Error {
        let s = error.to_string();
        let k = error.kind();
        std::io::Error::new(k, s)
    }

    pub async fn fail(self, error: std::io::Error) {
        let error_clone = Self::clone_err(&error);
        let _ = self.fail_error.write().await.insert(error_clone);
        let mut mutex = self.outgoing.lock().await;

        let keys: Vec<Vec<u8>> = mutex.keys().cloned().collect();
        for k in keys {
            let error_clone = Self::clone_err(&error);
            let _ = mutex.remove(&k).map(|resp| resp.send(Err(error_clone)));
        }
    }

    pub async fn get_sender(&self, key: &[u8]) -> Option<UnboundedSender<IoResult<Packet>>> {
        self.outgoing
            .lock()
            .await
            .get(key)
            .map(UnboundedSender::clone)
    }
    /// Sets the sender of a given key,
    /// returns the previous key if it was set
    pub async fn set_sender(
        &self,
        key: Vec<u8>,
        sender: UnboundedSender<IoResult<Packet>>,
    ) -> Option<UnboundedSender<IoResult<Packet>>> {
        self.check_for_error(&sender).await?;
        self.outgoing.lock().await.insert(key, sender)
    }
    pub async fn remove_sender(&self, key: &[u8]) -> Option<UnboundedSender<IoResult<Packet>>> {
        self.outgoing.lock().await.remove(key)
    }
}

async fn receive_loop<T: ClientTransport + 'static>(
    transport: Weak<T>,
    transport_sync: TransportSynchronizer,
) -> std::io::Result<()> {
    let err = loop {
        let Some(transport_instance) = transport.upgrade() else {
            // nobody else is listening so we can drop our reference
            return Ok(());
        };
        // we do a timeout here to ensure that we do not block forever
        let Ok(recv_res) = timeout(
            Duration::from_millis(300),
            transport_instance.receive_packet(),
        )
        .await
        else {
            continue;
        };
        let option_packet = match recv_res {
            Err(e) => break e,
            Ok(o) => o,
        };
        let Some(packet) = option_packet else {
            trace!("unexpected malformed packet received");
            continue;
        };
        if let Some(ack) = parse_for_ack(&packet) {
            transport_instance.send(&ack).await?;
        }

        let MessageClass::Response(_) = packet.message.header.code else {
            continue;
        };

        let token = packet.message.get_token();
        let Some(sender) = transport_sync.get_sender(token).await else {
            info!("received unexpected response for token {:?}", &token);
            continue;
        };
        match packet.message.header.code {
            MessageClass::Response(_) => {}
            m => {
                debug!("unknown message type {}", m);
                continue;
            }
        };
        let Ok(_) = sender.send(Ok(packet)) else {
            debug!("unexpected drop of sender");
            continue;
        };
    };

    let e = Err(Error::new(err.kind(), err.to_string()));
    transport_sync.fail(err).await;
    return e;
}

pub fn parse_for_ack(packet: &Packet) -> Option<Vec<u8>> {
    match (packet.message.header.get_type(), packet.message.header.code) {
        (MessageType::Confirmable, MessageClass::Response(_)) => Some(make_ack(packet)),
        _ => None,
    }
}

pub fn make_ack(packet: &Packet) -> Vec<u8> {
    let mut ack = Message::new();
    ack.header.set_type(MessageType::Acknowledgement);
    ack.header.message_id = packet.message.header.message_id;
    ack.header.code = MessageClass::Empty;
    return ack.to_bytes().unwrap();
}

/// a wrapper for transports responsible for retries and timeouts
struct CoapClientTransport<T: ClientTransport> {
    pub(crate) transport: Arc<T>,
    pub(crate) synchronizer: TransportSynchronizer,
    pub(crate) retries: usize,
    pub(crate) timeout: Duration,
}

impl<T: ClientTransport> Clone for CoapClientTransport<T> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            synchronizer: self.synchronizer.clone(),
            retries: self.retries.clone(),
            timeout: self.timeout.clone(),
        }
    }
}

impl<T: ClientTransport> CoapClientTransport<T> {
    pub const DEFAULT_NUM_RETRIES: usize = 5;
    async fn establish_receiver_for(&self, packet: &Packet) -> UnboundedReceiver<IoResult<Packet>> {
        let (tx, rx) = unbounded_channel();
        let token = packet.message.get_token().to_owned();
        self.synchronizer.set_sender(token, tx).await;
        return rx;
    }

    /// tries to send a confirmable message with retries and timeouts
    async fn try_send_confirmable_message(
        &self,
        msg: &Packet,
        receiver: &mut UnboundedReceiver<IoResult<Packet>>,
    ) -> IoResult<Packet> {
        let mut res = Err(Error::new(ErrorKind::InvalidData, "not enough retries"));
        for _ in 0..self.retries {
            res = self.try_send_non_confirmable_message(&msg, receiver).await;
            if res.is_ok() {
                return res;
            }
        }
        return res;
    }

    fn encode_message(message: &Message) -> IoResult<Vec<u8>> {
        message
            .to_bytes()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e.to_string()))
    }

    async fn try_send_non_confirmable_message(
        &self,
        msg: &Packet,
        receiver: &mut UnboundedReceiver<IoResult<Packet>>,
    ) -> IoResult<Packet> {
        let bytes = Self::encode_message(&msg.message)?;
        self.transport.send(&bytes).await?;
        let try_receive: Result<Option<Result<Packet, Error>>, tokio::time::error::Elapsed> =
            timeout(self.timeout, receiver.recv()).await;
        if let Ok(Some(res)) = try_receive {
            return res;
        }
        Err(Error::new(ErrorKind::TimedOut, "send timeout"))
    }

    async fn do_request_response_for_packet_inner(
        &self,
        packet: &Packet,
        receiver: &mut UnboundedReceiver<IoResult<Packet>>,
    ) -> IoResult<Packet> {
        if packet.message.header.get_type() == MessageType::Confirmable {
            return self.try_send_confirmable_message(&packet, receiver).await;
        } else {
            return self
                .try_send_non_confirmable_message(&packet, receiver)
                .await;
        }
    }

    pub async fn do_request_response_for_packet(&self, packet: &Packet) -> IoResult<Packet> {
        let mut receiver = self.establish_receiver_for(packet).await;
        let result = self
            .do_request_response_for_packet_inner(packet, &mut receiver)
            .await;
        self.synchronizer
            .remove_sender(packet.message.get_token())
            .await;
        result
    }

    pub fn from_transport(transport: Arc<T>, synchronizer: TransportSynchronizer) -> Self {
        return Self {
            transport,
            synchronizer,
            retries: Self::DEFAULT_NUM_RETRIES,
            timeout: Duration::from_secs(DEFAULT_RECEIVE_TIMEOUT_SECONDS),
        };
    }
}

pub struct UdpTransport {
    pub socket: UdpSocket,
    pub peer_addr: SocketAddr,
}
#[async_trait]
impl ClientTransport for UdpTransport {
    async fn recv(&self, buf: &mut [u8]) -> std::io::Result<(usize, Option<SocketAddr>)> {
        let (read, addr) = self.socket.recv_from(buf).await?;
        return Ok((read, Some(addr)));
    }
    async fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.socket.send_to(buf, self.peer_addr).await
    }
}

/// A CoAP client over UDP. This client can send multicast and broadcasts
pub type UdpCoAPClient = CoAPClient<UdpTransport>;

pub struct CoAPClient<T: ClientTransport> {
    transport: CoapClientTransport<T>,
    block1_size: usize,
    message_id: Arc<AtomicU16>,
}

impl<T: ClientTransport> Clone for CoAPClient<T> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            block1_size: self.block1_size.clone(),
            message_id: self.message_id.clone(),
        }
    }
}

/// a receiver used whenever you have a use case involving multiple responses to a single request
pub struct MessageReceiver {
    synchronizer: TransportSynchronizer,
    receiver: UnboundedReceiver<IoResult<Packet>>,
    token: Vec<u8>,
}

impl MessageReceiver {
    pub async fn receive(&mut self) -> IoResult<Packet> {
        match self.receiver.recv().await {
            Some(Ok(packet)) => Ok(packet),
            Some(Err(e)) => Err(e),
            None => Err(Error::new(
                ErrorKind::Other,
                "sender dropped by synchronizer",
            )),
        }
    }
    pub fn new(
        synchronizer: TransportSynchronizer,
        receiver: UnboundedReceiver<IoResult<Packet>>,
        token: &[u8],
    ) -> Self {
        Self {
            synchronizer,
            receiver,
            token: token.to_vec(),
        }
    }
}

impl Drop for MessageReceiver {
    fn drop(&mut self) {
        let sync = self.synchronizer.clone();
        let tok = std::mem::take(&mut self.token);
        tokio::spawn(async move { sync.remove_sender(&tok).await });
    }
}

impl UdpCoAPClient {
    pub async fn new_with_specific_source<A: ToSocketAddrs, B: ToSocketAddrs>(
        bind_addr: A,
        peer_addr: B,
    ) -> IoResult<Self> {
        let peer_addr = lookup_host(peer_addr).await?.next().ok_or(Error::new(
            ErrorKind::InvalidInput,
            "could not get socket address",
        ))?;
        let socket = UdpSocket::bind(bind_addr).await?;
        let transport = UdpTransport { socket, peer_addr };
        return Ok(UdpCoAPClient::from_transport(transport));
    }

    pub async fn new_udp<A: ToSocketAddrs>(addr: A) -> IoResult<Self> {
        let sock_addr = lookup_host(addr).await?.next().ok_or(Error::new(
            ErrorKind::InvalidInput,
            "could not get socket address",
        ))?;
        Ok(match &sock_addr {
            SocketAddr::V4(_) => Self::new_with_specific_source("0.0.0.0:0", sock_addr).await?,
            SocketAddr::V6(_) => Self::new_with_specific_source(":::0", sock_addr).await?,
        })
    }
    /// Send a request to all CoAP devices.
    /// - IPv4 AllCoAP multicast address is '224.0.1.187'
    /// - IPv6 AllCoAp multicast addresses are 'ff0?::fd'
    /// Parameter segment is used with IPv6 to determine the first octet.
    /// It's value can be between 0x0 and 0xf. To address multiple segments,
    /// you have to call send_all_coap for each of the segments.
    pub async fn send_all_coap(
        &self,
        request: &CoapRequest<SocketAddr>,
        segment: u8,
    ) -> IoResult<()> {
        assert!(segment <= 0xf);
        let addr = match self.transport.transport.peer_addr {
            SocketAddr::V4(val) => {
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 1, 187)), val.port())
            }
            SocketAddr::V6(val) => SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(
                    0xff00 + segment as u16,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0xfd,
                )),
                val.port(),
            ),
        };

        match request.message.to_bytes() {
            Ok(bytes) => {
                let size = self
                    .transport
                    .transport
                    .socket
                    .send_to(&bytes[..], addr)
                    .await?;
                if size == bytes.len() {
                    Ok(())
                } else {
                    Err(Error::new(ErrorKind::Other, "send length error"))
                }
            }
            Err(_) => Err(Error::new(ErrorKind::InvalidInput, "packet error")),
        }
    }

    pub fn set_broadcast(&self, value: bool) -> IoResult<()> {
        self.transport.transport.socket.set_broadcast(value)
    }

    /// creates a receiver based on a specific request
    /// this method can be used if you send a multicast request and
    /// expect multiple responses.
    /// only use this method if you know what you are doing
    /// ```
    ///
    /// use coap_lite::{
    ///     RequestType
    /// };
    /// use coap::request::RequestBuilder;
    /// use coap::client::UdpCoAPClient;
    ///
    /// async fn foo() {
    ///   let segment = 0x0;
    ///   let client = UdpCoAPClient::new_udp("127.0.0.1:5683")
    ///          .await
    ///          .unwrap();
    ///   let request = RequestBuilder::new("test-echo", RequestType::Get)
    ///       .data(Some(vec![0x51, 0x55, 0x77, 0xE8]))
    ///       .confirmable(true)
    ///       .build();
    ///
    ///   let mut receiver = client.create_receiver_for(&request).await;
    ///   client.send_all_coap(&request, segment).await.unwrap();
    ///   loop {
    ///      let recv_packet = receiver.receive().await.unwrap();
    ///      assert_eq!(recv_packet.message.payload, b"test-echo".to_vec());
    ///   }
    /// }
    /// ```

    pub async fn create_receiver_for(&self, request: &CoapRequest<SocketAddr>) -> MessageReceiver {
        let (tx, rx) = unbounded_channel();
        let key = request.message.get_token().to_vec();
        self.transport.synchronizer.set_sender(key, tx).await;
        return MessageReceiver::new(
            self.transport.synchronizer.clone(),
            rx,
            request.message.get_token(),
        );
    }
}

impl CoAPClient<DtlsConnection> {
    pub async fn from_udp_dtls_config(config: UdpDtlsConfig) -> IoResult<Self> {
        Ok(CoAPClient::from_transport(
            DtlsConnection::try_new(config).await?,
        ))
    }
}

impl<T: ClientTransport + 'static> CoAPClient<T> {
    const MAX_PAYLOAD_BLOCK: usize = 1024;
    /// Create a CoAP client with a chosen transport type

    pub fn from_transport(transport: T) -> Self {
        let synchronizer = TransportSynchronizer::new();
        let transport_arc = Arc::new(transport);
        let message_id: u16 = rand::random();
        // spawn receive loop to handle responses
        tokio::spawn(receive_loop(
            Arc::downgrade(&transport_arc),
            synchronizer.clone(),
        ));
        CoAPClient {
            transport: CoapClientTransport::from_transport(transport_arc.clone(), synchronizer),
            block1_size: Self::MAX_PAYLOAD_BLOCK,
            message_id: Arc::new(AtomicU16::new(message_id)),
        }
    }
    /// Execute a single get request with a coap url
    pub async fn get(url: &str) -> IoResult<CoapResponse> {
        Self::request(url, Method::Get, None).await
    }

    /// Execute a single get request with a coap url and a specific timeout.
    pub async fn get_with_timeout(url: &str, timeout: Duration) -> IoResult<CoapResponse> {
        Self::request_with_timeout(url, Method::Get, None, timeout).await
    }

    /// Execute a single post request with a coap url using udp
    pub async fn post(url: &str, data: Vec<u8>) -> IoResult<CoapResponse> {
        Self::request(url, Method::Post, Some(data)).await
    }

    /// Execute a single post request with a coap url using udp
    pub async fn post_with_timeout(
        url: &str,
        data: Vec<u8>,
        timeout: Duration,
    ) -> IoResult<CoapResponse> {
        Self::request_with_timeout(url, Method::Post, Some(data), timeout).await
    }

    /// Execute a put request with a coap url using udp
    pub async fn put(url: &str, data: Vec<u8>) -> IoResult<CoapResponse> {
        Self::request(url, Method::Put, Some(data)).await
    }

    /// Execute a single put request with a coap url using udp
    pub async fn put_with_timeout(
        url: &str,
        data: Vec<u8>,
        timeout: Duration,
    ) -> IoResult<CoapResponse> {
        Self::request_with_timeout(url, Method::Put, Some(data), timeout).await
    }

    /// Execute a single delete request with a coap url using udp
    pub async fn delete(url: &str) -> IoResult<CoapResponse> {
        Self::request(url, Method::Delete, None).await
    }

    /// Execute a single delete request with a coap url using udp
    pub async fn delete_with_timeout(url: &str, timeout: Duration) -> IoResult<CoapResponse> {
        Self::request_with_timeout(url, Method::Delete, None, timeout).await
    }

    /// Execute a single request (GET, POST, PUT, DELETE) with a coap url using udp
    pub async fn request(
        url: &str,
        method: Method,
        data: Option<Vec<u8>>,
    ) -> IoResult<CoapResponse> {
        let (domain, port, path, queries) = Self::parse_coap_url(url)?;
        let client = UdpCoAPClient::new_udp((domain.as_str(), port)).await?;
        let request = RequestBuilder::new(&path, method)
            .queries(queries)
            .domain(domain)
            .data(data)
            .build();
        client.send(request).await
    }

    /// Execute a single request (GET, POST, PUT, DELETE) with a coap url and a specfic timeout
    /// using udp
    pub async fn request_with_timeout(
        url: &str,
        method: Method,
        data: Option<Vec<u8>>,
        timeout: Duration,
    ) -> IoResult<CoapResponse> {
        let (domain, port, path, queries) = Self::parse_coap_url(url)?;
        let mut client = UdpCoAPClient::new_udp((domain.as_str(), port)).await?;
        client.set_receive_timeout(timeout);
        let request = RequestBuilder::new(&path, method)
            .queries(queries)
            .domain(domain)
            .data(data)
            .build();

        client.send(request).await
    }

    /// Send a Request via the given transport, and receive a response.
    /// users are responsible for filling meaningful fields in the request
    /// this method supports blockwise requests
    pub async fn send(&self, mut request: CoapRequest<SocketAddr>) -> IoResult<CoapResponse> {
        let first_response = self.send_request(&mut request).await?;
        request.response = Some(first_response);
        self.receive(&mut request).await
    }

    pub async fn observe<H: FnMut(Message) + Send + 'static>(
        &self,
        resource_path: &str,
        handler: H,
    ) -> IoResult<oneshot::Sender<ObserveMessage>>
    where
        T: 'static + Send + Sync,
    {
        let register_packet = RequestBuilder::new(resource_path, Method::Get).build();
        self.observe_with(register_packet, handler).await
    }

    /// Observe a resource with the handler and specified timeout using the given transport.
    /// Use the oneshot sender to cancel observation. If this sender is dropped without explicitly
    /// cancelling it, the observation will continue forever.
    pub async fn observe_with_timeout<H: FnMut(Message) + Send + 'static>(
        &mut self,
        resource_path: &str,
        handler: H,
        timeout: Duration,
    ) -> IoResult<oneshot::Sender<ObserveMessage>>
    where
        T: 'static + Send + Sync,
    {
        self.set_receive_timeout(timeout);
        self.observe(resource_path, handler).await
    }

    /// observe a resource with a given transport using your own request
    /// Use this method if you need to set some specific options in your
    /// requests. This method will add observe flags and a message id as a fallback
    /// Use this method if you plan on re-using the same client for requests
    pub async fn observe_with<H: FnMut(Message) + Send + 'static>(
        &self,
        request: CoapRequest<SocketAddr>,
        mut handler: H,
    ) -> IoResult<oneshot::Sender<ObserveMessage>> {
        let this = self.clone();
        let mut register_packet = request;
        if 0 == register_packet.message.header.message_id {
            register_packet.message.header.message_id = self.gen_message_id();
        }
        register_packet.set_observe_flag(ObserveOption::Register);

        let req_token = register_packet.message.get_token().to_vec();
        let resource_path = register_packet.get_path();
        let response = self.send(register_packet).await?;
        if *response.get_status() != Status::Content {
            return Err(Error::new(ErrorKind::NotFound, "the resource not found"));
        }
        let (tx_observe, mut rx_observe) = unbounded_channel();
        self.transport
            .synchronizer
            .set_sender(req_token.clone(), tx_observe)
            .await;

        handler(response.message);

        let (tx, rx) = oneshot::channel();
        let observe_path = String::from(resource_path);

        tokio::spawn(async move {
            let mut rx_pinned: Pin<
                Box<
                    dyn Future<
                            Output = std::result::Result<ObserveMessage, oneshot::error::RecvError>,
                        > + Send,
                >,
            > = Box::pin(rx);
            loop {
                tokio::select! {
                    sock_rx = rx_observe.recv() => {
                        Self::receive_and_handle_message_observe(sock_rx?, &mut handler).await;
                    }
                    observe = &mut rx_pinned => {
                        match observe {
                            Ok(ObserveMessage::Terminate) => {
                                this.terminate_observe(&observe_path, req_token).await;
                                break;
                            }
                            // if the receiver is dropped, we change the future to wait forever
                            Err(_) => {
                                debug!("observe continuing forever");
                                rx_pinned  = Box::pin(futures::future::pending())
                            },
                        }
                    }

                }
            }
            Some(())
        });
        return Ok(tx);
    }

    async fn terminate_observe(&self, observe_path: &str, req_token: Vec<u8>) {
        let mut deregister_packet = CoapRequest::<SocketAddr>::new();
        deregister_packet.message.header.message_id = self.gen_message_id();
        deregister_packet.set_observe_flag(ObserveOption::Deregister);
        deregister_packet.set_path(observe_path);
        deregister_packet.message.set_token(req_token);

        let _ = self
            .transport
            .do_request_response_for_packet(&Packet {
                address: None,
                message: deregister_packet.message,
            })
            .await;
    }

    async fn receive_and_handle_message_observe<H: FnMut(Message) + Send + 'static>(
        socket_result: IoResult<Packet>,
        handler: &mut H,
    ) {
        match socket_result {
            Ok(packet) => {
                handler(packet.message);
            }
            Err(e) => match e.kind() {
                ErrorKind::WouldBlock => {
                    info!("Observe timeout");
                }
                _ => warn!("observe failed {:?}", e),
            },
        };
    }

    /// sends a request through the transport. If a request is confirmable, it will attempt
    /// retries until receiving a response. requests sent using a multicast-address should be non-confirmable
    /// the user is responsible for setting meaningful fields in the request
    /// Do not use this method unless you need low-level control over the protocol (e.g.,
    /// multicast), instead use send for client applications.
    pub async fn send_single_request(
        &self,
        request: &CoapRequest<SocketAddr>,
    ) -> IoResult<CoapResponse> {
        let response = self
            .transport
            .do_request_response_for_packet(&Packet {
                address: None,
                message: request.message.to_owned(),
            })
            .await?;
        Ok(CoapResponse {
            message: response.message,
        })
    }

    /// low-level method to send a a request supporting block1 option based on
    /// the block size set in the client
    async fn send_request(&self, request: &mut CoapRequest<SocketAddr>) -> IoResult<CoapResponse> {
        let request_length = request.message.payload.len();
        if request_length <= self.block1_size {
            return self.send_single_request(request).await;
        }
        let payload = std::mem::take(&mut request.message.payload);
        let mut it = payload.chunks(self.block1_size).enumerate().peekable();
        let mut result = Err(Error::new(ErrorKind::Other, "unknown error occurred"));

        while let Some((idx, elem)) = it.next() {
            let more_blocks = it.peek().is_some();
            let block = BlockValue::new(idx, more_blocks, self.block1_size)
                .map_err(|_| Error::new(ErrorKind::Other, "could not set block size"))?;

            request.message.clear_option(CoapOption::Block1);
            request
                .message
                .add_option_as::<BlockValue>(CoapOption::Block1, block.clone());
            request.message.payload = elem.to_vec();

            request.message.header.message_id = self.gen_message_id();
            let resp = self.send_single_request(request).await?;
            // continue receiving responses until last element
            if it.peek().is_some() {
                let maybe_block1 = resp
                    .message
                    .get_first_option_as::<BlockValue>(CoapOption::Block1)
                    .ok_or(Error::new(
                        ErrorKind::Unsupported,
                        "endpoint does not support blockwise transfers. Try setting block1_size to a larger value",
                    ))?;
                let block1_resp = maybe_block1.map_err(|_| {
                    Error::new(
                        ErrorKind::InvalidData,
                        "endpoint responded with invalid block",
                    )
                })?;
                //TODO: negotiate smaller block size
                if block1_resp.size_exponent != block.size_exponent {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "negotiating block size is currently unsupported",
                    ));
                }
            }
            result = Ok(resp);
        }
        return result;
    }

    /// Receive a response support block-wise.
    async fn receive(&self, request: &mut CoapRequest<SocketAddr>) -> IoResult<CoapResponse> {
        let mut block2_state = BlockState::default();
        loop {
            match Self::intercept_response(request, &mut block2_state) {
                Ok(true) => {
                    let resp = self.send_single_request(request).await?;
                    request.response = Some(resp);
                }
                Err(err) => {
                    error!("intercept response error: {:?}", err);
                    return Err(Error::new(ErrorKind::Interrupted, "packet error"));
                }
                Ok(false) => {
                    break;
                }
            }
        }
        Ok(CoapResponse {
            message: request.response.as_ref().unwrap().message.clone(),
        })
    }

    /// Set the receive timeout.
    pub fn set_receive_timeout(&mut self, dur: Duration) {
        self.transport.timeout = dur;
    }

    pub fn set_transport_retries(&mut self, num_retries: usize) {
        self.transport.retries = num_retries;
    }

    /// Set the maximum size for a block1 request. Default is 1024 bytes
    pub fn set_block1_size(&mut self, block1_max_bytes: usize) {
        self.block1_size = block1_max_bytes;
    }

    fn parse_coap_url(url: &str) -> IoResult<(String, u16, String, Option<Vec<u8>>)> {
        let url_params = match Url::parse(url) {
            Ok(url_params) => url_params,
            Err(_) => return Err(Error::new(ErrorKind::InvalidInput, "url error")),
        };

        let host = match url_params.host_str() {
            Some("") => return Err(Error::new(ErrorKind::InvalidInput, "host error")),
            Some(h) => h,
            None => return Err(Error::new(ErrorKind::InvalidInput, "host error")),
        };
        let host = Regex::new(r"^\[(.*?)]$")
            .unwrap()
            .replace(&host, "$1")
            .to_string();

        let port = match url_params.port() {
            Some(p) => p,
            None => 5683,
        };

        let path = url_params.path().to_string();

        let queries = url_params.query().map(|q| q.as_bytes().to_vec());

        return Ok((host.to_string(), port, path, queries));
    }

    fn gen_message_id(&self) -> u16 {
        self.message_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn intercept_response(
        request: &mut CoapRequest<SocketAddr>,
        state: &mut BlockState,
    ) -> std::result::Result<bool, HandlingError> {
        let block2_handled = Self::maybe_handle_response_block2(request, state)?;
        if block2_handled {
            return Ok(true);
        }

        Ok(false)
    }

    fn maybe_handle_response_block2(
        request: &mut CoapRequest<SocketAddr>,
        state: &mut BlockState,
    ) -> std::result::Result<bool, HandlingError> {
        let response = request.response.as_ref().unwrap();
        let maybe_block2 = response
            .message
            .get_first_option_as::<BlockValue>(CoapOption::Block2)
            .and_then(|x| x.ok());

        if let Some(block2) = maybe_block2 {
            if state.cached_payload.is_none() {
                state.cached_payload = Some(Vec::new());
            }
            let cached_payload = state.cached_payload.as_mut().unwrap();

            let payload_offset = usize::from(block2.num) * block2.size();
            extending_splice(
                cached_payload,
                payload_offset..payload_offset + block2.size(),
                response.message.payload.iter().copied(),
                16 * 1024,
            )
            .map_err(HandlingError::internal)?;

            if block2.more {
                request.message.clear_option(CoapOption::Block2);
                let mut next_block2 = block2.clone();
                next_block2.num += 1;
                request
                    .message
                    .add_option_as::<BlockValue>(CoapOption::Block2, next_block2);
                return Ok(true);
            } else {
                let cached_payload = mem::take(&mut state.cached_payload).unwrap();
                request.response.as_mut().unwrap().message.payload = cached_payload;
            }
        }

        Ok(false)
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlockState {
    cached_payload: Option<Vec<u8>>,
}
