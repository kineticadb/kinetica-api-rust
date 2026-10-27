//! An address that refuses connections, for tests of what a refused
//! connection does.

use std::net::SocketAddr;

/// A loopback address that refuses connections for as long as the returned
/// socket lives.
///
/// The socket is bound but not listening, so it holds the port, and a
/// connection to it is answered with a reset. Another test cannot be given
/// the port in the meantime, as it could be if the port were only bound,
/// noted and released.
pub(crate) fn refusing_address() -> (socket2::Socket, SocketAddr) {
    let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
    socket.bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into()).unwrap();
    let address = socket.local_addr().unwrap().as_socket().unwrap();
    (socket, address)
}
