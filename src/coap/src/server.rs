use async_trait::async_trait;
use coap_lite::{BlockHandler, BlockHandlerConfig, CoapRequest, CoapResponse, Packet};
use tracing::debug;
use std::{
    self,
    future::Future,
    io::ErrorKind,
    net::{self, IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    sync::Arc,
};
use tokio::{
    io,
    net::UdpSocket,
    select,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task::JoinHandle,
};

use super::observer::Observer;

#[derive(Debug)]
pub enum CoAPServerError {
    NetworkError,
    EventLoopError,
    AnotherHandlerIsRunning,
    EventSendError,
}

use tokio::io::Error;

#[async_trait]
pub trait Dispatcher: Send + Sync {
    async fn dispatch(&self, request: CoapRequest<SocketAddr>) -> Option<CoapResponse>;
}

#[async_trait]
/// This trait represents a generic way to respond to a listener. If you want to implement your own
/// listener, you have to implement this trait to be able to send responses back through the
/// correct transport
pub trait Responder: Sync + Send {
    async fn respond(&self, response: Vec<u8>);
    fn address(&self) -> SocketAddr;
}

/// channel to send new requests from a transport to the CoAP server
pub type TransportRequestSender = UnboundedSender<(Vec<u8>, Arc<dyn Responder>)>;

/// channel used by CoAP server to receive new requests
pub type TransportRequestReceiver = UnboundedReceiver<(Vec<u8>, Arc<dyn Responder>)>;

type UdpResponseReceiver = UnboundedReceiver<(Vec<u8>, SocketAddr)>;
type UdpResponseSender = UnboundedSender<(Vec<u8>, SocketAddr)>;

// listeners receive new connections
#[async_trait]
pub trait Listener: Send {
    async fn listen(
        self: Box<Self>,
        sender: TransportRequestSender,
    ) -> std::io::Result<JoinHandle<std::io::Result<()>>>;
}
/// listener for a UDP socket
pub struct UdpCoapListener {
    socket: UdpSocket,
    multicast_addresses: Vec<IpAddr>,
    response_receiver: UdpResponseReceiver,
    response_sender: UdpResponseSender,
}

#[async_trait]
/// A trait for handling incoming requests. Use this instead of a closure
/// if you want to modify some external state
pub trait RequestHandler: Send + Sync + 'static {
    async fn handle_request(
        &self,
        mut request: Box<CoapRequest<SocketAddr>>,
    ) -> Box<CoapRequest<SocketAddr>>;
}

#[async_trait]
impl<F, HandlerRet> RequestHandler for F
where
    F: Fn(Box<CoapRequest<SocketAddr>>) -> HandlerRet + Send + Sync + 'static,
    HandlerRet: Future<Output = Box<CoapRequest<SocketAddr>>> + Send,
{
    async fn handle_request(
        &self,
        request: Box<CoapRequest<SocketAddr>>,
    ) -> Box<CoapRequest<SocketAddr>> {
        self(request).await
    }
}

/// A listener for UDP packets. This listener can also subscribe to multicast addresses
impl UdpCoapListener {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        let std_socket = net::UdpSocket::bind(addr)?;
        std_socket.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(std_socket)?;
        Ok(Self::from_socket(socket))
    }

    pub fn from_socket(socket: tokio::net::UdpSocket) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            socket,
            multicast_addresses: Vec::new(),
            response_receiver: rx,
            response_sender: tx,
        }
    }

    /// join multicast - adds the multicast addresses to the unicast listener
    /// - IPv4 multicast address range is '224.0.0.0/4'
    /// - IPv6 AllCoAp multicast addresses are 'ff00::/8'
    ///
    /// Parameter segment is used with IPv6 to determine the first octet.
    /// - It's value can be between 0x0 and 0xf.
    /// - To join multiple segments, you have to call enable_discovery for each of the segments.
    ///
    /// Some Multicast address scope
    /// IPv6        IPv4 equivalent[16]	        Scope	            Purpose
    /// ffx1::/16	127.0.0.0/8	                Interface-local	    Packets with this destination address may not be sent over any network link, but must remain within the current node; this is the multicast equivalent of the unicast loopback address.
    /// ffx2::/16	224.0.0.0/24	            Link-local	        Packets with this destination address may not be routed anywhere.
    /// ffx3::/16	239.255.0.0/16	            IPv4 local scope
    /// ffx4::/16	            	            Admin-local	        The smallest scope that must be administratively configured.
    /// ffx5::/16		                        Site-local	        Restricted to the local physical network.
    /// ffx8::/16	239.192.0.0/14	            Organization-local	Restricted to networks used by the organization administering the local network. (For example, these addresses might be used over VPNs; when packets for this group are routed over the public internet (where these addresses are not valid), they would have to be encapsulated in some other protocol.)
    /// ffxe::/16	224.0.1.0-238.255.255.255	Global scope	    Eligible to be routed over the public internet.
    ///
    /// Notable addresses:
    /// ff02::1	    All nodes on the local network segment
    /// ff0x::c	    Simple Service Discovery Protocol
    /// ff0x::fb	Multicast DNS
    /// ff0x::fb	Multicast CoAP
    /// ff0x::114	Used for experiments
    //    pub fn join_multicast(&mut self, addr: IpAddr) {
    //        self.udp_server.join_multicast(addr);
    //    }
    pub fn join_multicast(&mut self, addr: IpAddr) {
        assert!(addr.is_multicast());
        // determine wether IPv4 or IPv6 and
        // join the appropriate multicast address
        match self.socket.local_addr().unwrap() {
            SocketAddr::V4(val) => {
                match addr {
                    IpAddr::V4(ipv4) => {
                        let i = val.ip().clone();
                        self.socket.join_multicast_v4(ipv4, i).unwrap();
                        self.multicast_addresses.push(addr);
                    }
                    IpAddr::V6(_ipv6) => { /* handle IPv6 */ }
                }
            }
            SocketAddr::V6(_val) => {
                match addr {
                    IpAddr::V4(_ipv4) => { /* handle IPv4 */ }
                    IpAddr::V6(ipv6) => {
                        self.socket.join_multicast_v6(&ipv6, 0).unwrap();
                        self.multicast_addresses.push(addr);
                        //self.socket.set_only_v6(true)?;
                    }
                }
            }
        }
    }

    /// leave multicast - remove the multicast address from the listener
    pub fn leave_multicast(&mut self, addr: IpAddr) {
        assert!(addr.is_multicast());
        // determine wether IPv4 or IPv6 and
        // leave the appropriate multicast address
        match self.socket.local_addr().unwrap() {
            SocketAddr::V4(val) => {
                match addr {
                    IpAddr::V4(ipv4) => {
                        let i = val.ip().clone();
                        self.socket.leave_multicast_v4(ipv4, i).unwrap();
                        let index = self
                            .multicast_addresses
                            .iter()
                            .position(|&item| item == addr)
                            .unwrap();
                        self.multicast_addresses.remove(index);
                    }
                    IpAddr::V6(_ipv6) => { /* handle IPv6 */ }
                }
            }
            SocketAddr::V6(_val) => {
                match addr {
                    IpAddr::V4(_ipv4) => { /* handle IPv4 */ }
                    IpAddr::V6(ipv6) => {
                        self.socket.leave_multicast_v6(&ipv6, 0).unwrap();
                        let index = self
                            .multicast_addresses
                            .iter()
                            .position(|&item| item == addr)
                            .unwrap();
                        self.multicast_addresses.remove(index);
                    }
                }
            }
        }
    }
    /// enable AllCoAP multicasts - adds the AllCoap addresses to the listener
    /// - IPv4 AllCoAP multicast address is '224.0.1.187'
    /// - IPv6 AllCoAp multicast addresses are 'ff0?::fd'
    ///
    /// Parameter segment is used with IPv6 to determine the first octet.
    /// - It's value can be between 0x0 and 0xf.
    /// - To join multiple segments, you have to call enable_discovery for each of the segments.
    ///
    /// For further details see method join_multicast
    pub fn enable_all_coap(&mut self, segment: u8) {
        assert!(segment <= 0xf);
        let m = match self.socket.local_addr().unwrap() {
            SocketAddr::V4(_val) => IpAddr::V4(Ipv4Addr::new(224, 0, 1, 187)),
            SocketAddr::V6(_val) => IpAddr::V6(Ipv6Addr::new(
                0xff00 + segment as u16,
                0,
                0,
                0,
                0,
                0,
                0,
                0xfd,
            )),
        };
        self.join_multicast(m);
    }
}
#[async_trait]
impl Listener for UdpCoapListener {
    async fn listen(
        mut self: Box<Self>,
        sender: TransportRequestSender,
    ) -> std::io::Result<JoinHandle<std::io::Result<()>>> {
        return Ok(tokio::spawn(self.receive_loop(sender)));
    }
}

#[derive(Clone)]
struct UdpResponder {
    address: SocketAddr, // this is the address we are sending to
    tx: UdpResponseSender,
}

#[async_trait]
impl Responder for UdpResponder {
    async fn respond(&self, response: Vec<u8>) {
        let _ = self.tx.send((response, self.address));
    }
    fn address(&self) -> SocketAddr {
        self.address
    }
}

impl UdpCoapListener {
    pub async fn receive_loop(mut self, sender: TransportRequestSender) -> std::io::Result<()> {
        loop {
            let mut recv_vec = Vec::with_capacity(u16::MAX as usize);
            select! {
                message =self.socket.recv_buf_from(&mut recv_vec)=> {
                    match message {
                        Ok((_size, from)) => {
                            sender.send((recv_vec, Arc::new(UdpResponder{address: from, tx: self.response_sender.clone()}))).map_err( |_| std::io::Error::new(ErrorKind::Other, "server channel error"))?;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                },
                response = self.response_receiver.recv() => {
                    if let Some((bytes, to)) = response{
                        debug!("sending {:?} to {:?}", &bytes,  &to);
                        self.socket.send_to(&bytes, to).await?;
                    }
                    else {
                        // in case nobody is listening to us, we can just terminate, though this
                        // should never happen for UDP
                        return Ok(());
                    }

                }
            }
        }
    }
}

#[derive(Debug)]
pub struct QueuedMessage {
    pub address: SocketAddr,
    pub message: Packet,
}

struct ServerCoapState {
    observer: Observer,
    block_handler: BlockHandler<SocketAddr>,
    disable_observe: bool,
}

pub enum ShouldForwardToHandler {
    True,
    False,
}

impl ServerCoapState {
    pub async fn intercept_request(
        &mut self,
        request: &mut CoapRequest<SocketAddr>,
        responder: Arc<dyn Responder>,
    ) -> ShouldForwardToHandler {
        match self.block_handler.intercept_request(request) {
            Ok(true) => return ShouldForwardToHandler::False,
            Err(_err) => return ShouldForwardToHandler::False,
            Ok(false) => {}
        };

        if self.disable_observe {
            return ShouldForwardToHandler::True;
        }

        let should_be_forwarded = self.observer.request_handler(request, responder).await;
        if should_be_forwarded {
            return ShouldForwardToHandler::True;
        } else {
            return ShouldForwardToHandler::False;
        }
    }

    pub async fn intercept_response(&mut self, request: &mut CoapRequest<SocketAddr>) {
        match self.block_handler.intercept_response(request) {
            Err(err) => {
                let _ = request.apply_from_error(err);
            }
            _ => {}
        }
    }
    pub fn new() -> Self {
        Self {
            observer: Observer::new(),
            block_handler: BlockHandler::new(BlockHandlerConfig::default()),
            disable_observe: false,
        }
    }
    pub fn disable_observe_handling(&mut self, value: bool) {
        self.disable_observe = value
    }
}

pub struct Server {
    listeners: Vec<Box<dyn Listener>>,
    coap_state: Arc<Mutex<ServerCoapState>>,
    new_packet_receiver: TransportRequestReceiver,
    new_packet_sender: TransportRequestSender,
}

impl Server {
    /// Creates a CoAP server listening on the given address.
    pub fn new_udp<A: ToSocketAddrs>(addr: A) -> Result<Self, io::Error> {
        let listener: Vec<Box<dyn Listener>> = vec![Box::new(UdpCoapListener::new(addr)?)];
        Ok(Self::from_listeners(listener))
    }

    pub fn from_listeners(listeners: Vec<Box<dyn Listener>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Server {
            listeners,
            coap_state: Arc::new(Mutex::new(ServerCoapState::new())),
            new_packet_receiver: rx,
            new_packet_sender: tx,
        }
    }

    async fn spawn_handles(
        listeners: Vec<Box<dyn Listener>>,
        sender: TransportRequestSender,
    ) -> std::io::Result<Vec<JoinHandle<std::io::Result<()>>>> {
        let mut handles = vec![];
        for listener in listeners.into_iter() {
            let handle = listener.listen(sender.clone()).await?;
            handles.push(handle);
        }
        return Ok(handles);
    }

    /// run the server.
    pub async fn run<Handler: RequestHandler>(mut self, handler: Handler) -> Result<(), io::Error> {
        let _handles = Self::spawn_handles(self.listeners, self.new_packet_sender.clone()).await?;

        let handler_arc = Arc::new(handler);
        // receive an input, sync our cache / states, then call custom handler
        loop {
            let (bytes, respond) =
                self.new_packet_receiver.recv().await.ok_or_else(|| {
                    std::io::Error::new(ErrorKind::Other, "listen channel closed")
                })?;
            if let Ok(packet) = Packet::from_bytes(&bytes) {
                let mut request = Box::new(CoapRequest::<SocketAddr>::from_packet(
                    packet,
                    respond.address(),
                ));
                let mut coap_state = self.coap_state.lock().await;
                let should_forward = coap_state
                    .intercept_request(&mut request, respond.clone())
                    .await;

                match should_forward {
                    ShouldForwardToHandler::True => {
                        let handler_clone = handler_arc.clone();
                        let coap_state_clone = self.coap_state.clone();
                        tokio::spawn(async move {
                            request = handler_clone.handle_request(request).await;
                            coap_state_clone
                                .lock()
                                .await
                                .intercept_response(request.as_mut())
                                .await;

                            Self::respond_to_request(request, respond).await;
                        });
                    }
                    ShouldForwardToHandler::False => {
                        Self::respond_to_request(request, respond).await;
                    }
                }
            }
        }
    }
    async fn respond_to_request(req: Box<CoapRequest<SocketAddr>>, responder: Arc<dyn Responder>) {
        // if we have some reponse to send, send it
        if let Some(Ok(b)) = req.response.map(|resp| resp.message.to_bytes()) {
            responder.respond(b).await;
        }
    }
    /// disable auto-observe handling in server
    pub async fn disable_observe_handling(&mut self, value: bool) {
        let mut coap_state = self.coap_state.lock().await;
        coap_state.disable_observe_handling(value)
    }
}