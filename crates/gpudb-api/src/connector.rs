//! The TCP connections the HTTP client makes.
//!
//! Each address of the host is tried in turn, each with the full connect
//! timeout, rather than raced. The socket has `TCP_NODELAY` set, and
//! `SO_KEEPALIVE` as the options say (on by default, with the operating
//! system's timings), with any keep-alive idle time, interval and probe count
//! the options give. An option the platform does not support is skipped.
//!
//! A connection lives for the settings' time to live from its opening: past
//! that it is not reused, though a request already on it finishes.

use hyper::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use log::debug;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

/// How connections are made: the connect timeout and the keep-alive options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TcpSettings {
    /// How long to wait for each address to accept; `None` waits as long as
    /// the operating system does.
    pub connect_timeout: Option<Duration>,
    /// Whether `SO_KEEPALIVE` is set.
    pub keep_alive: bool,
    /// The keep-alive idle time, in seconds; 0 leaves the system's.
    pub keep_idle: u32,
    /// The keep-alive probe interval, in seconds; 0 leaves the system's.
    pub keep_interval: u32,
    /// The keep-alive probe count; 0 leaves the system's.
    pub keep_count: u32,
    /// How long after its opening a connection may still be reused.
    pub time_to_live: Duration,
}

/// The connector the HTTP client's TLS layer wraps.
#[derive(Debug, Clone)]
pub(crate) struct TcpConnector {
    settings: Arc<TcpSettings>,
}

impl TcpConnector {
    pub(crate) fn new(settings: TcpSettings) -> Self {
        TcpConnector { settings: Arc::new(settings) }
    }
}

impl tower_service::Service<Uri> for TcpConnector {
    type Response = OpenedStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let settings = self.settings.clone();
        Box::pin(async move {
            let stream = connect(&uri, &settings).await?;
            let clock = ConnectionClock {
                opened_at: Instant::now(),
                time_to_live: settings.time_to_live,
                armed: Arc::new(AtomicBool::new(false)),
            };
            Ok(OpenedStream { io: TokioIo::new(stream), clock })
        })
    }
}

/// A TCP connection, with when it was opened.
pub(crate) struct OpenedStream {
    io: TokioIo<TcpStream>,
    clock: ConnectionClock,
}

impl Connection for OpenedStream {
    /// What the pool knows of the connection, with its [`ConnectionClock`],
    /// which every response on it carries.
    fn connected(&self) -> Connected {
        self.io.connected().extra(self.clock.clone())
    }
}

/// When a connection was opened, carried in the extensions of every
/// response on it, so that the client can retire the connection at its time
/// to live (see [`ConnectionClock::arm`]).
#[derive(Debug, Clone)]
pub(crate) struct ConnectionClock {
    opened_at: Instant,
    time_to_live: Duration,
    armed: Arc<AtomicBool>,
}

impl ConnectionClock {
    /// The time left to the connection's time to live, the first time it is
    /// asked; `None` after that. The client asks on each response, and on
    /// the first sets a timer to poison the connection then, which keeps the
    /// pool from handing it out again.
    pub(crate) fn arm(&self) -> Option<Duration> {
        (!self.armed.swap(true, Ordering::Relaxed)).then(|| self.time_to_live.saturating_sub(self.opened_at.elapsed()))
    }
}

impl hyper::rt::Read for OpenedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for OpenedStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write_vectored(cx, bufs)
    }
}

/// Connects to the first address of `uri`'s host that accepts, trying each
/// in turn.
async fn connect(uri: &Uri, settings: &TcpSettings) -> io::Result<TcpStream> {
    let host = uri
        .host()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("URI has no host: {uri}")))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("https") { 443 } else { 80 });
    let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();

    let mut last_error = None;
    for address in addresses {
        let attempt = TcpStream::connect(address);
        let connected = match settings.connect_timeout {
            Some(limit) => tokio::time::timeout(limit, attempt).await.unwrap_or_else(|_| {
                Err(io::Error::new(io::ErrorKind::TimedOut, format!("Connect to {address} timed out")))
            }),
            None => attempt.await,
        };
        match connected {
            Ok(stream) => {
                configure(&stream, settings)?;
                return Ok(stream);
            }
            Err(e) => last_error = Some(e),
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("No address found for {host}"))))
}

/// Sets the socket's options.
fn configure(stream: &TcpStream, settings: &TcpSettings) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let socket = socket2::SockRef::from(stream);
    socket.set_keepalive(settings.keep_alive)?;
    if !settings.keep_alive {
        return Ok(());
    }

    let seconds = |s: u32| Duration::from_secs(u64::from(s));
    if settings.keep_idle > 0 {
        match socket.set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(seconds(settings.keep_idle))) {
            Ok(()) => debug!("Set TCP_KEEPIDLE to {} seconds", settings.keep_idle),
            Err(e) => debug!("TCP_KEEPIDLE not supported on this platform: {e}"),
        }
    }
    if settings.keep_interval > 0 {
        match set_keep_interval(&socket, seconds(settings.keep_interval)) {
            Ok(()) => debug!("Set TCP_KEEPINTERVAL to {} seconds", settings.keep_interval),
            Err(e) => debug!("TCP_KEEPINTERVAL not supported on this platform: {e}"),
        }
    }
    if settings.keep_count > 0 {
        match set_keep_count(&socket, settings.keep_count) {
            Ok(()) => debug!("Set TCP_KEEPCOUNT to {}", settings.keep_count),
            Err(e) => debug!("TCP_KEEPCOUNT not supported on this platform: {e}"),
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "netbsd", windows))]
fn set_keep_interval(socket: &socket2::SockRef<'_>, interval: Duration) -> io::Result<()> {
    socket.set_tcp_keepalive(&socket2::TcpKeepalive::new().with_interval(interval))
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "netbsd", windows)))]
fn set_keep_interval(_: &socket2::SockRef<'_>, _: Duration) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "not supported"))
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "netbsd"))]
fn set_keep_count(socket: &socket2::SockRef<'_>, count: u32) -> io::Result<()> {
    socket.set_tcp_keepalive(&socket2::TcpKeepalive::new().with_retries(count))
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "netbsd")))]
fn set_keep_count(_: &socket2::SockRef<'_>, _: u32) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "not supported"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    fn settings() -> TcpSettings {
        TcpSettings { connect_timeout: Some(Duration::from_secs(2)), keep_alive: true, keep_idle: 0, keep_interval: 0, keep_count: 0, time_to_live: Duration::from_secs(60) }
    }

    async fn connected(settings: TcpSettings) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri: Uri = format!("http://{}/", listener.local_addr().unwrap()).parse().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        connect(&uri, &settings).await.expect("connected")
    }

    // The defaults: no Nagle, and keep-alive on with the system's timings.
    #[tokio::test]
    async fn a_connection_keeps_alive_with_the_systems_timings_by_default() {
        let stream = connected(settings()).await;
        let socket = socket2::SockRef::from(&stream);
        assert!(stream.nodelay().unwrap());
        assert!(socket.keepalive().unwrap());
        #[cfg(target_os = "linux")]
        {
            let system: u64 = std::fs::read_to_string("/proc/sys/net/ipv4/tcp_keepalive_time").unwrap().trim().parse().unwrap();
            assert_eq!(socket.tcp_keepalive_time().unwrap(), Duration::from_secs(system));
        }
    }

    #[tokio::test]
    async fn the_keep_alive_timings_are_set_as_given() {
        let stream = connected(TcpSettings { keep_idle: 30, keep_interval: 5, keep_count: 3, ..settings() }).await;
        let socket = socket2::SockRef::from(&stream);
        assert!(socket.keepalive().unwrap());
        #[cfg(target_os = "linux")]
        {
            assert_eq!(socket.tcp_keepalive_time().unwrap(), Duration::from_secs(30));
            assert_eq!(socket.tcp_keepalive_interval().unwrap(), Duration::from_secs(5));
            assert_eq!(socket.tcp_keepalive_retries().unwrap(), 3);
        }
    }

    #[tokio::test]
    async fn keep_alive_can_be_turned_off() {
        let stream = connected(TcpSettings { keep_alive: false, keep_idle: 30, ..settings() }).await;
        assert!(!socket2::SockRef::from(&stream).keepalive().unwrap());
    }

    #[tokio::test]
    async fn a_refused_connection_is_reported() {
        let (_held, address) = crate::test_net::refusing_address();
        let uri: Uri = format!("http://{address}/").parse().unwrap();
        let err = connect(&uri, &settings()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
    }
}
