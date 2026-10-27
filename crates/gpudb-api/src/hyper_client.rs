//! High-performance HTTP client using hyper directly.
//!
//! This module provides a custom HTTP client that bypasses reqwest's abstraction
//! layers for maximum performance, particularly for bulk insert operations.

use crate::KineticaError;
use hyper::{body::{Bytes, Incoming}, Request, Response, Uri};
use hyper_rustls::HttpsConnector;
use crate::connector::{ConnectionClock, TcpConnector, TcpSettings};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use http_body_util::{BodyExt, Full};
use log::{debug, error, trace, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The connect and response timeouts of the fast-connect client, which quick
/// status checks use.
pub(crate) const FAST_CONNECTION_TIMEOUT: Duration = Duration::from_millis(2000);

/// High-performance HTTP client using hyper with connection pooling.
///
/// This client is optimized for:
/// - HTTP/2 multiplexing
/// - Aggressive connection reuse
/// - Minimal abstraction overhead
#[derive(Clone)]
pub struct HyperClient {
    client: Client<HttpsConnector<TcpConnector>, RequestBody>,
    /// The fast-connect client: its own connection pool, connecting within
    /// [`FAST_CONNECTION_TIMEOUT`]. Quick status checks go through it, without
    /// retries.
    fast_client: Client<HttpsConnector<TcpConnector>, RequestBody>,
    request_timeout: Option<Duration>,
    connection_request_timeout: Duration,
    /// See [`HttpClientConfig::max_retries`].
    max_retries: usize,
    /// Caps on concurrent API requests.
    limits: Arc<ConnectionLimits>,
    /// Caps on concurrent health checks. Health checks have their own
    /// connection pool, apart from API requests', so a pool saturated by
    /// long-running requests cannot make a live server look down.
    health_check_limits: Arc<ConnectionLimits>,
    /// Set by [`Self::close`], and shared by every clone.
    closed: Arc<std::sync::atomic::AtomicBool>,
}

impl Default for HyperClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Connection settings for a [`HyperClient`].
///
/// [`HttpClientConfig::from_options`] derives them from `KineticaOptions`.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Accept any server certificate. See [`AcceptAnyServerCert`] for what
    /// this gives up.
    pub bypass_ssl_cert_check: bool,
    /// The trust store to trust servers by, in place of the system's roots
    pub trust_store: Option<Arc<crate::TrustStore>>,
    /// Limit on establishing a connection; `None` waits indefinitely.
    pub connect_timeout: Option<Duration>,
    /// Read timeout: the limit on the wait for a response once the request
    /// has been sent, and then on each read of its body; the upload is not
    /// limited. `None` waits indefinitely.
    pub request_timeout: Option<Duration>,
    /// Most requests in flight to one host at a time; also the number of idle
    /// connections kept per host for reuse. At least 1.
    pub max_connections_per_host: usize,
    /// Most requests in flight across all hosts at a time. At least 1.
    pub max_total_connections: usize,
    /// How long a request waits for a free connection when a limit above is
    /// reached, before failing as a connection error.
    pub connection_request_timeout: Duration,
    /// How many times an API request is retried, at once and on the same
    /// server: when sending it fails without a response (the connection is
    /// refused, reset or closed, the host is unknown, or no response arrives
    /// within the timeout), or the response is a 502 or 503. A connect
    /// timeout, a TLS failure or a failure reading the response body is not
    /// retried, and neither are health checks.
    pub max_retries: usize,
    /// Whether connections send TCP keep-alive probes: on by default with the
    /// operating system's timings.
    pub tcp_keep_alive: bool,
    /// Seconds a connection idles before the first probe; 0 keeps the
    /// system's.
    pub tcp_keep_idle: u32,
    /// Seconds between probes; 0 keeps the system's.
    pub tcp_keep_interval: u32,
    /// Probes before the connection is dropped; 0 keeps the system's.
    pub tcp_keep_count: u32,
    /// How long after its opening a connection may be reused:
    /// [`CONNECTION_TIME_TO_LIVE`], one minute, by default.
    pub connection_time_to_live: Duration,
}

/// How long a pooled connection may sit idle before it is closed: 100
/// seconds.
const EVICT_IDLE_CONNECTIONS_AFTER: Duration = Duration::from_secs(100);

/// How long after its opening a connection may be reused: one minute.
pub const CONNECTION_TIME_TO_LIVE: Duration = Duration::from_secs(60);

/// How long a request waits for a free connection: 5 minutes.
pub const DEFAULT_CONNECTION_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self::from_options(&crate::KineticaOptions::default())
    }
}

impl HttpClientConfig {
    /// Takes the connection settings from `options`:
    ///
    /// - `server_connection_timeout` is the connect timeout,
    /// - `timeout` is the request timeout,
    /// - `max_connections_per_host` and `max_total_connections` cap the
    ///   requests in flight.
    ///
    /// A timeout of 0 means no limit. A connection limit below 1, which
    /// `Kinetica` refuses, is raised to 1 here for a configuration built some
    /// other way.
    pub fn from_options(options: &crate::KineticaOptions) -> Self {
        let millis = |ms: u64| (ms > 0).then(|| Duration::from_millis(ms));
        Self {
            bypass_ssl_cert_check: options.bypass_ssl_cert_check,
            trust_store: None,
            connect_timeout: millis(options.server_connection_timeout),
            request_timeout: millis(options.timeout),
            max_connections_per_host: options.max_connections_per_host.max(1),
            max_total_connections: options.max_total_connections.max(1),
            connection_request_timeout: DEFAULT_CONNECTION_REQUEST_TIMEOUT,
            max_retries: options.max_retries,
            tcp_keep_alive: options.tcp_keep_alive,
            tcp_keep_idle: options.tcp_keep_idle,
            tcp_keep_interval: options.tcp_keep_interval,
            tcp_keep_count: options.tcp_keep_count,
            connection_time_to_live: CONNECTION_TIME_TO_LIVE,
        }
    }
}

/// Caps on concurrent requests, in total and per host.
///
/// hyper-util's pool has no such caps, so they are enforced here. A request
/// holds one per-host and one total permit from before it is sent until its
/// body has been read.
///
/// These caps can throttle high-concurrency callers. With the defaults (10 per
/// host, 40 in total), a `BulkInserter` running more than 10 flush workers
/// against one host, or more than 40 across the cluster, queues the extra
/// requests here instead of sending them. Raise
/// `KineticaOptions::max_connections_per_host` / `max_total_connections` to
/// match the insert concurrency.
struct ConnectionLimits {
    total: Arc<Semaphore>,
    max_per_host: usize,
    /// One semaphore per `scheme://host:port`, created on first use. Entries
    /// are never removed; a client talks to a fixed, small set of cluster
    /// hosts.
    per_host: parking_lot::Mutex<HashMap<String, Arc<Semaphore>>>,
}

/// Permission to send one request, released when dropped.
struct ConnectionPermit {
    _host: OwnedSemaphorePermit,
    _total: OwnedSemaphorePermit,
}

impl ConnectionLimits {
    fn new(max_total: usize, max_per_host: usize) -> Self {
        Self {
            total: Arc::new(Semaphore::new(max_total.max(1))),
            max_per_host: max_per_host.max(1),
            per_host: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Waits for room under both caps. The per-host permit is taken first, so
    /// a request queued for a busy host does not hold a total permit that
    /// requests to other hosts could use.
    async fn acquire(&self, uri: &Uri) -> ConnectionPermit {
        let route = format!(
            "{}://{}",
            uri.scheme_str().unwrap_or("http"),
            uri.authority().map(|a| a.as_str()).unwrap_or("")
        );
        let host = self
            .per_host
            .lock()
            .entry(route)
            .or_insert_with(|| Arc::new(Semaphore::new(self.max_per_host)))
            .clone();

        // Neither semaphore is ever closed, so acquiring cannot fail.
        let host = host.acquire_owned().await.expect("connection semaphore is never closed");
        let total = self.total.clone().acquire_owned().await.expect("connection semaphore is never closed");
        ConnectionPermit { _host: host, _total: total }
    }
}

/// A response as received, before its status is interpreted.
///
/// Kinetica can report an error with a non-2xx status and still send an Avro
/// body that says what went wrong, so the caller needs both; see
/// `Kinetica::submit_request_raw`, which interprets them.
#[derive(Debug, Clone)]
pub struct RawResponse {
    /// HTTP status code
    pub status: u16,
    /// The reason phrase the server sent with the status, e.g. "Bad Gateway"
    /// or "Kinetica is exiting"
    pub reason: String,
    /// `Content-Type` header, if present
    pub content_type: Option<String>,
    /// Response body
    pub body: Bytes,
}

/// The TLS failure a request failed with, if it did: a failed handshake or
/// a rejected certificate, wherever it sits in the error's cause chain.
///
/// Those are reported as a plain error rather than as a connection failure,
/// so they do not trigger a failover: another cluster would present the same
/// certificate problem.
fn tls_error(error: &(dyn std::error::Error + 'static)) -> Option<rustls::Error> {
    let mut current = Some(error);
    while let Some(e) = current {
        // An io::Error does not expose what it wraps through `source()`, and
        // the TLS connector's may wrap another
        let mut wrapped: &(dyn std::error::Error + 'static) = e;
        loop {
            if let Some(tls) = wrapped.downcast_ref::<rustls::Error>() {
                return Some(tls.clone());
            }
            match wrapped.downcast_ref::<std::io::Error>().and_then(|io| io.get_ref()) {
                Some(inner) => wrapped = inner,
                None => break,
            }
        }
        current = e.source();
    }
    None
}

/// The end of a TLS failure's message: its advice on how to fix it.
const SSL_ERROR_MESSAGE_FORMAT: &str = concat!(
    ">.  To fix, either:\n",
    "* Specify a trust store containing the server's certificate or a CA cert in the server's certificate path\n",
    "* Skip the certificate check using the bypass_ssl_cert_check option\n",
    "  Examples:  https://docs.kinetica.com/7.2/api/concepts/#https-without-certificate-validation",
);

/// The message for a TLS failure reaching `url`: a certificate not for the
/// server's name, one from no trusted issuer, or any other failure, each with
/// advice on how to fix it.
fn ssl_error_message(url: &str, error: &rustls::Error) -> String {
    use rustls::CertificateError;
    let reason = match error {
        rustls::Error::InvalidCertificate(
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
        ) => "Server SSL certificate not found in specified trust store.".to_string(),
        rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer) => {
            "No trust store was specified, but server requires SSL certificate validation.".to_string()
        }
        other => format!("Error: {other}"),
    };
    format!("Encountered SSL error when trying to connect to server at {url}.  <{reason}{SSL_ERROR_MESSAGE_FORMAT}")
}

/// Sends `req` on `client`. On a connection's first response, a timer is set
/// to retire the connection at its time to live (see
/// [`crate::connector::ConnectionClock`]).
async fn send(
    client: &Client<HttpsConnector<TcpConnector>, RequestBody>,
    mut req: Request<RequestBody>,
) -> Result<Response<Incoming>, hyper_util::client::legacy::Error> {
    let captured = hyper_util::client::legacy::connect::capture_connection(&mut req);
    let resp = client.request(req).await?;
    if let Some(remaining) = resp.extensions().get::<ConnectionClock>().and_then(ConnectionClock::arm) {
        tokio::spawn(async move {
            tokio::time::sleep(remaining).await;
            if let Some(connected) = captured.connection_metadata().as_ref() {
                connected.poison();
            }
        });
    }
    Ok(resp)
}

/// The innermost error under `error`, where the real reason is. For a refused
/// connection, hyper's "client error (Connect)" becomes the operating
/// system's "Connection refused".
fn root_cause(error: &(dyn std::error::Error + 'static)) -> String {
    let mut current = error;
    while let Some(source) = current.source() {
        current = source;
    }
    current.to_string()
}

/// Awaits `stage`, giving up after `limit`.
///
/// A timeout is reported as [`KineticaError::Connection`]: the server has
/// stopped responding, which the HA logic handles like an unreachable server.
async fn within<T>(
    limit: Option<Duration>,
    what: &str,
    url: &str,
    stage: impl std::future::Future<Output = T>,
) -> Result<T, KineticaError> {
    match limit {
        None => Ok(stage.await),
        Some(limit) => tokio::time::timeout(limit, stage).await.map_err(|_| {
            KineticaError::Connection(format!(
                "Timed out after {} ms waiting for {what} from {url}",
                limit.as_millis()
            ))
        }),
    }
}

/// Awaits the response to a request, giving up `limit` after the request has
/// been sent, so an upload of any length is never cut off. `sent` is the
/// request body's signal (see [`RequestBody::tracked`]). Fails as [`within`]
/// does.
async fn response_within<T>(
    limit: Option<Duration>,
    url: &str,
    request: impl std::future::Future<Output = T>,
    sent: tokio::sync::oneshot::Receiver<()>,
) -> Result<T, KineticaError> {
    let Some(limit) = limit else {
        return Ok(request.await);
    };
    tokio::pin!(request);
    tokio::select! {
        response = &mut request => return Ok(response),
        _ = sent => {}
    }
    within(Some(limit), "a response", url, request).await
}

/// A request body that reports when the HTTP client is done with it.
///
/// hyper drops a body once it has written it, after flushing it to the
/// connection where it is too large to buffer, or once it gives up on the
/// request; [`RequestBody::tracked`] signals then. So that the drop comes
/// after the write, this reports more to come until its data has been taken:
/// told the stream had ended, hyper would drop it as soon as it had buffered
/// the data.
pub(crate) struct RequestBody {
    body: Full<Bytes>,
    taken: bool,
    sent: Option<tokio::sync::oneshot::Sender<()>>,
}

impl RequestBody {
    /// A body of `bytes`, and the signal that it has been sent.
    fn tracked(bytes: Bytes) -> (Self, tokio::sync::oneshot::Receiver<()>) {
        let (sent, signal) = tokio::sync::oneshot::channel();
        (Self { body: Full::new(bytes), taken: false, sent: Some(sent) }, signal)
    }

    /// An empty body, with no signal.
    fn empty() -> Self {
        Self { body: Full::new(Bytes::new()), taken: false, sent: None }
    }
}

impl hyper::body::Body for RequestBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let frame = std::pin::Pin::new(&mut this.body).poll_frame(cx);
        if let std::task::Poll::Ready(Some(_)) = &frame {
            this.taken = true;
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        !self.taken && self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        if self.taken {
            hyper::body::SizeHint::with_exact(0)
        } else {
            self.body.size_hint()
        }
    }
}

impl Drop for RequestBody {
    fn drop(&mut self) {
        if let Some(sent) = self.sent.take() {
            let _ = sent.send(());
        }
    }
}

/// A certificate verifier that accepts anything.
///
/// Installed only when the caller sets
/// `KineticaOptions::set_bypass_ssl_cert_check(true)`. It skips both the check
/// that the certificate is trusted and the check of the host name it is for.
///
/// This disables authentication of the server: the connection is still
/// encrypted, but nothing proves it is the intended server, so it offers no
/// protection against interception. It exists because on-prem deployments
/// commonly present self-signed certificates.
#[derive(Debug)]
struct AcceptAnyServerCert(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

impl HyperClient {
    /// Creates a new HyperClient with the default [`HttpClientConfig`].
    ///
    /// Since the server doesn't support HTTP/2, we optimize for HTTP/1.1:
    /// - Pooled connections, with a long idle timeout to maximize reuse
    /// - TCP_NODELAY for low latency
    /// - Fast connection establishment
    pub fn new() -> Self {
        Self::with_config(&HttpClientConfig::default())
    }

    /// Creates a new `HyperClient` with the default [`HttpClientConfig`],
    /// optionally accepting any server certificate.
    ///
    /// # Arguments
    ///
    /// * `bypass_ssl_cert_check` - Accept any server certificate. See
    ///   [`AcceptAnyServerCert`] for what this gives up.
    pub fn with_tls_options(bypass_ssl_cert_check: bool) -> Self {
        Self::with_config(&HttpClientConfig { bypass_ssl_cert_check, ..HttpClientConfig::default() })
    }

    /// Creates a new `HyperClient` with the given settings.
    ///
    /// The connector speaks both `http` and `https`, chosen per request from
    /// the URL scheme, so a plaintext deployment is unaffected by TLS being
    /// available.
    pub fn with_config(config: &HttpClientConfig) -> Self {
        let extended_keep_alive =
            config.tcp_keep_idle > 0 || config.tcp_keep_interval > 0 || config.tcp_keep_count > 0;
        if !config.tcp_keep_alive && extended_keep_alive {
            warn!(
                "TCP keep-alive is disabled (tcp_keep_alive=false), but extended TCP keep-alive options are set. \
                 These options will be ignored: tcp_keep_idle={}, tcp_keep_interval={}, tcp_keep_count={}. \
                 To use extended options, set tcp_keep_alive=true.",
                config.tcp_keep_idle, config.tcp_keep_interval, config.tcp_keep_count
            );
        }
        debug!("Setting TCP keep-alive to <{}>", config.tcp_keep_alive);

        let client = Self::build_client(config, config.connect_timeout);
        let fast_client = Self::build_client(config, Some(FAST_CONNECTION_TIMEOUT));

        let limits = || Arc::new(ConnectionLimits::new(
            config.max_total_connections,
            config.max_connections_per_host,
        ));

        Self {
            client,
            fast_client,
            request_timeout: config.request_timeout,
            connection_request_timeout: config.connection_request_timeout,
            max_retries: config.max_retries,
            limits: limits(),
            health_check_limits: limits(),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Stops this client, and every clone of it, from sending a POST: each
    /// fails from then on, without failing over. A ping still goes out.
    pub(crate) fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Builds a pooled HTTP client connecting within `connect_timeout`.
    fn build_client(
        config: &HttpClientConfig,
        connect_timeout: Option<Duration>,
    ) -> Client<HttpsConnector<TcpConnector>, RequestBody> {
        // Connections are made as the connector module describes
        let connector = TcpConnector::new(TcpSettings {
            connect_timeout,
            keep_alive: config.tcp_keep_alive,
            keep_idle: config.tcp_keep_idle,
            keep_interval: config.tcp_keep_interval,
            keep_count: config.tcp_keep_count,
            time_to_live: config.connection_time_to_live,
        });

        let https = Self::build_https_connector(connector, config.bypass_ssl_cert_check, config.trust_store.as_ref());

        // HTTP/1.1 cannot multiplex, so concurrent requests each need their own
        // connection; keeping them idle for reuse avoids a new TCP (and TLS)
        // handshake per request. Note that hyper-util reads 0 here as "keep no
        // idle connections", not "unlimited".
        Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(config.max_connections_per_host.max(1))
            // A connection's time to live is kept by the connector
            .pool_idle_timeout(EVICT_IDLE_CONNECTIONS_AFTER)
            .http2_only(false)  // Allow HTTP/1.1
            .build(https)
    }

    /// Builds the TLS-capable connector.
    ///
    /// Trust roots come from the operating system where they can be read. The
    /// bundled Mozilla root set is the fallback for environments with no
    /// system store.
    fn build_https_connector(
        http: TcpConnector,
        bypass_ssl_cert_check: bool,
        trust_store: Option<&Arc<crate::TrustStore>>,
    ) -> HttpsConnector<TcpConnector> {
        let builder = hyper_rustls::HttpsConnectorBuilder::new();

        // A trust store in place of the system's roots
        if let (false, Some(store)) = (bypass_ssl_cert_check, trust_store) {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let verifier = crate::trust_store::TrustStoreVerifier::new(store.clone(), provider.clone());
            let tls_config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .expect("the ring provider supports the default protocol versions")
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth();
            return builder.with_tls_config(tls_config).https_or_http().enable_all_versions().wrap_connector(http);
        }

        // Logged once, at debug, when the connection's options are read
        // (trust_store::from_options)
        let tls_config = if bypass_ssl_cert_check {
            let provider = rustls::crypto::ring::default_provider();
            rustls::ClientConfig::builder_with_provider(Arc::new(provider.clone()))
                .with_safe_default_protocol_versions()
                .expect("the ring provider supports the default protocol versions")
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert(Arc::new(
                    provider,
                ))))
                .with_no_client_auth()
        } else {
            // `with_native_roots` fails when there is no system trust store to
            // read; fall back rather than refuse every HTTPS connection.
            match hyper_rustls::HttpsConnectorBuilder::new().with_native_roots() {
                Ok(b) => {
                    debug!("TLS trust roots loaded from the system store");
                    return b.https_or_http().enable_all_versions().wrap_connector(http);
                }
                Err(e) => {
                    debug!(
                        "Could not load system TLS trust roots ({e}); \
                         falling back to the bundled Mozilla root set"
                    );
                    return builder
                        .with_webpki_roots()
                        .https_or_http()
                        .enable_all_versions()
                        .wrap_connector(http);
                }
            }
        };

        hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_all_versions()
            .wrap_connector(http)
    }

    /// Sends a POST request with binary body.
    ///
    /// This is optimized for Avro binary payloads sent to Kinetica.
    ///
    /// # Arguments
    ///
    /// * `url` - Full URL to send request to
    /// * `body` - Binary payload (Avro-encoded)
    /// * `headers` - The headers to send besides the content type; see
    ///   [`RequestHeaders`]
    ///
    /// # Returns
    ///
    /// Response body as bytes
    pub async fn post_binary(
        &self,
        url: &str,
        body: Vec<u8>,
        headers: &RequestHeaders,
    ) -> Result<Bytes, KineticaError> {
        self.post_binary_with_compression(url, body, headers, false).await
    }

    /// Sends a POST request with binary body, optionally compressed with Snappy.
    ///
    /// This is optimized for Avro binary payloads sent to Kinetica.
    ///
    /// # Arguments
    ///
    /// * `url` - Full URL to send request to
    /// * `body` - Binary payload (Avro-encoded)
    /// * `headers` - The headers to send besides the content type; see
    ///   [`RequestHeaders`]
    /// * `use_snappy` - Whether to compress the body with Snappy
    ///
    /// # Returns
    ///
    /// Response body as bytes
    ///
    /// # Compression
    ///
    /// When `use_snappy` is true:
    /// - The body is compressed using Snappy compression
    /// - Content-Type is set to `application/x-snappy`
    /// - This can significantly reduce transfer time for large payloads
    pub async fn post_binary_with_compression(
        &self,
        url: &str,
        body: Vec<u8>,
        headers: &RequestHeaders,
        use_snappy: bool,
    ) -> Result<Bytes, KineticaError> {
        let resp = self.post_binary_raw(url, body, headers, use_snappy).await?;

        // Check status after reading body
        // Note: Kinetica typically returns 200 OK even for API errors (error is in Avro response)
        // But some endpoints return 4xx with error details in body
        if !(200..300).contains(&resp.status) {
            // Try to extract error message from body if it's text
            let body_str = String::from_utf8_lossy(&resp.body);
            let status = format!("{} {}", resp.status, resp.reason);
            return if body_str.is_empty() {
                // The variant's Display adds the "HTTP error: " prefix.
                Err(KineticaError::Http(status))
            } else {
                Err(KineticaError::Http(format!("{status} - {body_str}")))
            }
        }

        Ok(resp.body)
    }

    /// Sends a POST request like [`Self::post_binary_with_compression`], but
    /// returns the response whatever its status, for the caller to interpret.
    ///
    /// Failures are classified as follows:
    ///
    /// - Failing to send the request or to receive a response, including a
    ///   timeout, is a [`KineticaError::Connection`], which fails over.
    /// - A TLS failure is a [`KineticaError::Config`], which does not: another
    ///   cluster would present the same certificate problem.
    /// - Failing to read the body, including a timeout, is a
    ///   [`KineticaError::Http`].
    ///
    /// Before any of that, a failed send or a 502 or 503 is retried up to
    /// [`HttpClientConfig::max_retries`] times; the last outcome is what is
    /// classified.
    pub async fn post_binary_raw(
        &self,
        url: &str,
        body: Vec<u8>,
        headers: &RequestHeaders,
        use_snappy: bool,
    ) -> Result<RawResponse, KineticaError> {
        self.send_post(Route::Main, url, Payload::Binary { body, use_snappy }, headers).await
    }

    /// Sends a POST request like [`Self::post_binary_raw`], with a body that is
    /// already encoded, and already Snappy-compressed if `snappy`.
    ///
    /// For a caller that builds the body off the async runtime, compression
    /// included, and may send it more than once: `Bytes` clones without
    /// copying the payload.
    pub(crate) async fn post_encoded_raw(
        &self,
        url: &str,
        body: Bytes,
        snappy: bool,
        headers: &RequestHeaders,
    ) -> Result<RawResponse, KineticaError> {
        self.send_post(Route::Main, url, Payload::Encoded { body, snappy }, headers).await
    }

    /// Sends an encoded, uncompressed body once, with no retries, waiting at
    /// most `limit` for the response and again for its body: the one
    /// best-effort POST that drops a session for a connection that was never
    /// closed. Its connect timeout is the connection's, which `limit` bounds
    /// as well.
    pub(crate) async fn post_encoded_once(
        &self,
        url: &str,
        body: Bytes,
        headers: &RequestHeaders,
        limit: Duration,
    ) -> Result<RawResponse, KineticaError> {
        self.send_post(Route::Once(limit), url, Payload::Encoded { body, snappy: false }, headers).await
    }

    /// Sends a POST with a JSON body, or with none, and returns the response
    /// whatever its status; failures are classified as
    /// [`Self::post_binary_raw`] classifies them.
    pub async fn post_json_raw(
        &self,
        url: &str,
        json: Option<String>,
        headers: &RequestHeaders,
    ) -> Result<RawResponse, KineticaError> {
        let payload = match json {
            Some(json) => Payload::Json(json),
            None => Payload::Empty,
        };
        self.send_post(Route::Main, url, payload, headers).await
    }

    /// Sends a POST request like [`Self::post_binary_raw`], through the fast
    /// client, for a quick status check: its own connection pool and limits, a
    /// connect timeout and a response timeout of [`FAST_CONNECTION_TIMEOUT`]
    /// each, and no retries.
    ///
    /// The two timeouts cannot be told apart here, so the wait for a response
    /// is bounded by both together.
    pub async fn post_binary_raw_fast(
        &self,
        url: &str,
        body: Vec<u8>,
        headers: &RequestHeaders,
    ) -> Result<RawResponse, KineticaError> {
        self.send_post(Route::Fast, url, Payload::Binary { body, use_snappy: false }, headers).await
    }

    async fn send_post(
        &self,
        route: Route,
        url: &str,
        payload: Payload,
        headers: &RequestHeaders,
    ) -> Result<RawResponse, KineticaError> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KineticaError::Config(format!(
                "Cannot submit a request to {url}: the connection has been closed"
            )));
        }

        // Each limit here bounds the wait for the response once the request is
        // sent, and then each read of its body, never the upload.
        let (client, limits, response_limit, body_limit, max_retries) = match route {
            Route::Fast => (
                &self.fast_client,
                &self.health_check_limits,
                Some(FAST_CONNECTION_TIMEOUT),
                Some(FAST_CONNECTION_TIMEOUT),
                0,
            ),
            Route::Main => (&self.client, &self.limits, self.request_timeout, self.request_timeout, self.max_retries),
            Route::Once(limit) => (&self.client, &self.limits, Some(limit), Some(limit), 0),
        };
        // Parse URI
        let uri: Uri = url.parse()
            .map_err(|e| KineticaError::Http(format!("Invalid URL: {e}")))?;

        // Optionally compress with Snappy and set appropriate Content-Type
        let (content_type, body_bytes) = match payload {
            Payload::Binary { body, use_snappy: true } => {
                (Some("application/x-snappy"), Bytes::from(snappy_compress(&body)?))
            }
            Payload::Binary { body, use_snappy: false } => (Some("application/octet-stream"), Bytes::from(body)),
            Payload::Json(json) => (Some("application/json; charset=UTF-8"), Bytes::from(json)),
            Payload::Empty => (None, Bytes::new()),
            Payload::Encoded { body, snappy: true } => (Some("application/x-snappy"), body),
            Payload::Encoded { body, snappy: false } => (Some("application/octet-stream"), body),
        };

        // Executions are counted from 1, and a retry is made while the count
        // is at most max_retries, so a request is sent up to max_retries + 1
        // times, with no pause in between.
        let mut execution_count = 1;
        // A request that cannot be sent fails with its time from the first
        // try, its retries included, and the root cause, logged at ERROR: the
        // innermost error's own text.
        let started = Instant::now();
        let submit_failed = |cause: String| {
            let elapsed = started.elapsed().as_millis();
            error!("Giving up with an exit error after {elapsed}ms due to {cause}");
            KineticaError::Connection(format!("Error submitting endpoint <{url}> request after <{elapsed}>ms due to: {cause}"))
        };
        let (_permit, resp) = loop {
            // Build request
            let mut builder = Request::post(uri.clone());
            for (name, value) in &headers.user {
                builder = builder.header(name.as_str(), value.as_str());
            }
            if let Some(session) = &headers.session {
                builder = builder.header("X-Kinetica-Session", session.as_str());
            }
            if let Some(mode) = &headers.ha_sync_mode {
                builder = builder.header("X-Kinetica-Group", mode.as_str());
            }
            if let Some(authorization) = &headers.authorization {
                builder = builder.header("Authorization", authorization.as_str());
            }
            if let Some(content_type) = content_type {
                builder = builder.header("Content-type", content_type);
            }
            let (request_body, sent) = RequestBody::tracked(body_bytes.clone());
            let req = builder
                .body(request_body)
                .map_err(|e| KineticaError::Http(format!("Failed to build request: {e}")))?;

            // Wait for room under the connection limits, then send. The permit
            // is held until the body has been read.
            let permit = within(
                Some(self.connection_request_timeout),
                "a free connection",
                url,
                limits.acquire(req.uri()),
            )
            .await
            .map_err(|timeout| submit_failed(timeout.message()))?;
            let may_retry = execution_count <= max_retries;
            let resp = match response_within(response_limit, url, send(client, req), sent).await {
                // No response in time, retried.
                Err(timeout) => {
                    trace!("Retry #{execution_count} after error {timeout}");
                    if may_retry {
                        execution_count += 1;
                        continue;
                    }
                    return Err(submit_failed(timeout.message()));
                }
                Ok(Err(e)) => {
                    trace!("Retry #{execution_count} after error {e}");
                    if let Some(tls) = tls_error(&e) {
                        let message = ssl_error_message(url, &tls);
                        error!("{message}");
                        return Err(KineticaError::Config(message));
                    }
                    // A connect timeout included
                    if may_retry {
                        execution_count += 1;
                        continue;
                    }
                    return Err(submit_failed(root_cause(&e)));
                }
                Ok(Ok(resp)) => resp,
            };
            let status = resp.status().as_u16();
            if may_retry && (status == 502 || status == 503) {
                trace!("Retry #{execution_count} for status {status}");
                execution_count += 1;
                continue;
            }
            break (permit, resp);
        };

        let status = resp.status();
        // hyper keeps the phrase only when it is not the standard one
        let reason = resp
            .extensions()
            .get::<hyper::ext::ReasonPhrase>()
            .map(|phrase| String::from_utf8_lossy(phrase.as_bytes()).into_owned())
            .unwrap_or_else(|| status.canonical_reason().unwrap_or("").to_string());
        let content_type = resp
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        // Read the body even on an error status: it may say what went wrong.
        // A failure part way through is not a connection failure, so a body
        // timeout is re-labelled too. The timeout bounds each read rather than
        // the whole body: a large response still arriving is not cut off.
        let mut incoming = resp.into_body();
        let mut parts: Vec<Bytes> = Vec::new();
        loop {
            match within(body_limit, "the response body", url, incoming.frame()).await {
                Err(timeout) => return Err(KineticaError::Http(timeout.to_string())),
                Ok(None) => break,
                Ok(Some(Err(e))) => return Err(KineticaError::Http(format!("Failed to read response: {e}"))),
                Ok(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data() {
                        parts.push(data);
                    }
                }
            }
        }
        let body = match parts.len() {
            0 => Bytes::new(),
            1 => parts.pop().unwrap_or_default(),
            _ => Bytes::from(parts.concat()),
        };

        Ok(RawResponse {
            status: status.as_u16(),
            reason,
            content_type,
            body,
        })
    }

    /// Sends a GET request and returns the response body as text.
    ///
    /// This is used for simple health checks like pinging the server.
    ///
    /// # Arguments
    ///
    /// * `url` - Full URL to send request to
    ///
    /// # Returns
    ///
    /// Response body as a string
    pub async fn get_text(&self, url: &str) -> Result<String, KineticaError> {
        self.get_text_within(url, &self.limits, Some(self.connection_request_timeout), self.request_timeout)
            .await
    }

    /// Like [`Self::get_text`], for health checks that must answer quickly:
    /// `limit` replaces the client's timeouts, and the request goes through
    /// the separate health-check connection limits.
    pub async fn get_text_with_timeout(&self, url: &str, limit: Duration) -> Result<String, KineticaError> {
        self.get_text_within(url, &self.health_check_limits, Some(limit), Some(limit)).await
    }

    /// Sends a GET with `headers` and returns the body as text, whatever the
    /// status; `limit` bounds the response and the body each, `None` waiting
    /// as long as it takes.
    pub async fn ping(&self, url: &str, headers: &RequestHeaders, limit: Option<Duration>) -> Result<String, KineticaError> {
        self.ping_answer(url, headers, limit).await.map(|(_, _, body)| body)
    }

    /// [`Self::ping`], with the status and reason phrase of the answer.
    pub(crate) async fn ping_answer(
        &self,
        url: &str,
        headers: &RequestHeaders,
        limit: Option<Duration>,
    ) -> Result<(u16, String, String), KineticaError> {
        let uri: Uri = url.parse().map_err(|e| KineticaError::Http(format!("Invalid URL: {e}")))?;
        let mut builder = Request::get(uri);
        for (name, value) in &headers.user {
            builder = builder.header(name.as_str(), value.as_str());
        }
        if let Some(session) = &headers.session {
            builder = builder.header("X-Kinetica-Session", session.as_str());
        }
        if let Some(mode) = &headers.ha_sync_mode {
            builder = builder.header("X-Kinetica-Group", mode.as_str());
        }
        if let Some(authorization) = &headers.authorization {
            builder = builder.header("Authorization", authorization.as_str());
        }
        let req = builder
            .body(RequestBody::empty())
            .map_err(|e| KineticaError::Http(format!("Failed to build request: {e}")))?;

        let _permit = within(limit, "a free connection", url, self.health_check_limits.acquire(req.uri())).await?;
        let resp = within(limit, "a response", url, send(&self.client, req))
            .await?
            .map_err(|e| KineticaError::Http(format!("Request failed: {e}")))?;
        let status = resp.status();
        let reason = resp
            .extensions()
            .get::<hyper::ext::ReasonPhrase>()
            .map(|phrase| String::from_utf8_lossy(phrase.as_bytes()).into_owned())
            .unwrap_or_else(|| status.canonical_reason().unwrap_or("").to_string());
        let body = within(limit, "the response body", url, resp.into_body().collect())
            .await?
            .map_err(|e| KineticaError::Http(format!("Failed to read response: {e}")))?
            .to_bytes();
        Ok((status.as_u16(), reason, String::from_utf8_lossy(&body).into_owned()))
    }

    async fn get_text_within(
        &self,
        url: &str,
        limits: &ConnectionLimits,
        wait_limit: Option<Duration>,
        limit: Option<Duration>,
    ) -> Result<String, KineticaError> {
        // Parse URI
        let uri: Uri = url.parse()
            .map_err(|e| KineticaError::Http(format!("Invalid URL: {e}")))?;

        // Build request
        let req = Request::get(uri)
            .header("Connection", "keep-alive")
            .body(RequestBody::empty())
            .map_err(|e| KineticaError::Http(format!("Failed to build request: {e}")))?;

        let _permit = within(wait_limit, "a free connection", url, limits.acquire(req.uri())).await?;
        let resp = within(limit, "a response", url, send(&self.client, req))
            .await?
            .map_err(|e| KineticaError::Http(format!("Request failed: {e}")))?;

        // Read response body
        let body = within(limit, "the response body", url, resp.into_body().collect())
            .await?
            .map_err(|e| KineticaError::Http(format!("Failed to read response: {e}")))?
            .to_bytes();

        // Convert to string
        String::from_utf8(body.to_vec())
            .map_err(|e| KineticaError::Http(format!("Invalid UTF-8 response: {e}")))
    }
}

/// Which client, limits and timeouts a POST goes through.
#[derive(Debug, Clone, Copy)]
enum Route {
    /// The main client, with the connection's timeout and retries.
    Main,
    /// The fast client, for a quick status check: see
    /// [`HyperClient::post_binary_raw_fast`].
    Fast,
    /// The main client, once, waiting at most this long for the response and
    /// again for its body: see [`HyperClient::post_encoded_once`].
    Once(Duration),
}

/// What a POST carries.
enum Payload {
    /// Avro binary, Snappy-compressed or not.
    Binary { body: Vec<u8>, use_snappy: bool },
    /// JSON text.
    Json(String),
    /// Nothing.
    Empty,
    /// A finished Avro body, already Snappy-compressed if `snappy`.
    Encoded { body: Bytes, snappy: bool },
}

/// Compresses an Avro body with Snappy.
pub(crate) fn snappy_compress(body: &[u8]) -> Result<Vec<u8>, KineticaError> {
    snap::raw::Encoder::new()
        .compress_vec(body)
        .map_err(|e| KineticaError::Http(format!("Snappy compression failed: {e}")))
}

/// The headers sent with a request besides its content type.
#[derive(Debug, Clone, Default)]
pub struct RequestHeaders {
    /// The caller's headers, the `User-Agent` among them.
    pub user: Vec<(String, String)>,
    /// The connection's session ID, sent as `X-Kinetica-Session`: SQL
    /// `#name` temp tables belong to the session that creates them.
    pub session: Option<String>,
    /// The HA synchronicity mode, sent as `X-Kinetica-Group` when it is not
    /// the default.
    pub ha_sync_mode: Option<String>,
    /// The `Authorization` header, if any; see [`authorization_header`].
    pub authorization: Option<String>,
}

/// The `Authorization` header to send: `Bearer <token>` when an OAuth token
/// is set, otherwise `Basic` with the credentials when the username or the
/// password is set, and no header at all when neither is.
///
/// The credentials are encoded as UTF-8.
pub fn authorization_header(oauth_token: &str, username: &str, password: &str) -> Option<String> {
    if !oauth_token.is_empty() {
        return Some(format!("Bearer {oauth_token}"));
    }
    if !username.is_empty() || !password.is_empty() {
        let credentials = format!("{username}:{password}");
        return Some(format!("Basic {}", base64::encode(credentials.as_bytes())));
    }
    None
}

// Need base64 for basic auth
pub(crate) mod base64 {
    /// Decodes MIME base64: characters outside the alphabet are skipped,
    /// decoding stops at padding, and a malformed ending fails.
    pub(crate) fn decode_mime(input: &str) -> Result<Vec<u8>, String> {
        fn value(c: u8) -> Option<u32> {
            match c {
                b'A'..=b'Z' => Some(u32::from(c - b'A')),
                b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
                b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
                b'+' => Some(62),
                b'/' => Some(63),
                _ => None,
            }
        }

        let src = input.as_bytes();
        let mut out = Vec::new();
        let (mut bits, mut shift_to, mut sp) = (0u32, 18i32, 0usize);
        while sp < src.len() {
            let c = src[sp];
            sp += 1;
            if c == b'=' {
                let wrong_ending = || Err("Base64 padding '=' is misplaced".to_string());
                // Padding after a whole unit ends nothing
                if shift_to == 18 {
                    return wrong_ending();
                }
                // "xx=" must be followed by a second "="
                if shift_to == 6 {
                    if src.get(sp) != Some(&b'=') {
                        return wrong_ending();
                    }
                    sp += 1;
                }
                break;
            }
            let Some(v) = value(c) else { continue };
            bits |= v << shift_to;
            shift_to -= 6;
            if shift_to < 0 {
                out.extend_from_slice(&[(bits >> 16) as u8, (bits >> 8) as u8, bits as u8]);
                shift_to = 18;
                bits = 0;
            }
        }
        match shift_to {
            6 => out.push((bits >> 16) as u8),
            0 => out.extend_from_slice(&[(bits >> 16) as u8, (bits >> 8) as u8]),
            12 => return Err("Base64 text ends with a lone character".to_string()),
            _ => {}
        }
        while sp < src.len() {
            sp += 1;
            if value(src[sp - 1]).is_some() {
                return Err(format!("Base64 text continues after its padding, at character {sp}"));
            }
        }
        Ok(out)
    }

    pub fn encode(input: &[u8]) -> String {
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        let mut result = String::new();
        let mut i = 0;

        while i + 2 < input.len() {
            let b1 = input[i];
            let b2 = input[i + 1];
            let b3 = input[i + 2];

            result.push(CHARS[(b1 >> 2) as usize] as char);
            result.push(CHARS[(((b1 & 0x03) << 4) | (b2 >> 4)) as usize] as char);
            result.push(CHARS[(((b2 & 0x0f) << 2) | (b3 >> 6)) as usize] as char);
            result.push(CHARS[(b3 & 0x3f) as usize] as char);

            i += 3;
        }

        if i < input.len() {
            let b1 = input[i];
            result.push(CHARS[(b1 >> 2) as usize] as char);

            if i + 1 < input.len() {
                let b2 = input[i + 1];
                result.push(CHARS[(((b1 & 0x03) << 4) | (b2 >> 4)) as usize] as char);
                result.push(CHARS[((b2 & 0x0f) << 2) as usize] as char);
                result.push('=');
            } else {
                result.push(CHARS[((b1 & 0x03) << 4) as usize] as char);
                result.push('=');
                result.push('=');
            }
        }

        result
    }
}

/// The TLS connector must build in both configurations.
///
/// These cannot exercise a handshake without a server, but they do guard the
/// part most likely to break silently: the `rustls` builder chain. A version
/// bump that changes it would otherwise surface only as every HTTPS connection
/// failing at run time, and `bypass_ssl_cert_check` reverting to the no-op it
/// used to be.
#[cfg(test)]
mod tls_tests {
    use super::*;

    /// The TLS connector wraps its failure in two I/O errors, the inner of
    /// which `source()` does not reach.
    #[test]
    fn a_tls_failure_is_found_through_nested_io_errors() {
        let tls = rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        let wrapped = std::io::Error::other(std::io::Error::new(std::io::ErrorKind::InvalidData, tls));
        assert!(matches!(
            tls_error(&wrapped),
            Some(rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer))
        ));
    }

    #[test]
    fn tls_failures_are_reported_by_what_failed() {
        let advice = ">.  To fix, either:\n* Specify a trust store containing the server's certificate or a CA cert \
                      in the server's certificate path\n* Skip the certificate check using the bypass_ssl_cert_check \
                      option\n  Examples:  https://docs.kinetica.com/7.2/api/concepts/#https-without-certificate-validation";
        let url = "https://h:8082/gpudb-0/show/table";
        assert_eq!(
            ssl_error_message(url, &rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer)),
            format!(
                "Encountered SSL error when trying to connect to server at {url}.  \
                 <No trust store was specified, but server requires SSL certificate validation.{advice}"
            )
        );
        assert_eq!(
            ssl_error_message(url, &rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)),
            format!(
                "Encountered SSL error when trying to connect to server at {url}.  \
                 <Server SSL certificate not found in specified trust store.{advice}"
            )
        );
        assert!(ssl_error_message(url, &rustls::Error::InvalidCertificate(rustls::CertificateError::Expired))
            .contains("<Error: invalid peer certificate: Expired>"));
    }

    #[test]
    fn a_client_builds_with_certificate_verification() {
        let _ = HyperClient::with_tls_options(false);
    }

    #[test]
    fn a_client_builds_with_verification_bypassed() {
        let _ = HyperClient::with_tls_options(true);
    }

    #[test]
    fn the_default_client_verifies_certificates() {
        // `new()` must not be the permissive one: bypassing verification has to
        // stay something a caller opts into.
        let _ = HyperClient::new();
    }

    /// The permissive verifier must accept what a real one rejects — otherwise
    /// `bypass_ssl_cert_check` would be wired up but inert, which is the state
    /// this whole change exists to fix.
    #[test]
    fn the_permissive_verifier_accepts_an_arbitrary_certificate() {
        use rustls::client::danger::ServerCertVerifier;

        let verifier = AcceptAnyServerCert(Arc::new(rustls::crypto::ring::default_provider()));
        let bogus = rustls_pki_types::CertificateDer::from(vec![0u8; 8]);

        let verdict = verifier.verify_server_cert(
            &bogus,
            &[],
            &rustls_pki_types::ServerName::try_from("example.invalid").unwrap(),
            &[],
            rustls_pki_types::UnixTime::now(),
        );

        assert!(verdict.is_ok(), "the bypass verifier must accept any certificate");
        assert!(
            !verifier.supported_verify_schemes().is_empty(),
            "signature schemes must still come from the crypto provider"
        );
    }
}

/// Timeouts and connection reuse, against a local listener that stands in for
/// a server that is slow, silent, or well-behaved.
#[cfg(test)]
mod connection_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// One attempt per request, so each test sees a single exchange; the
    /// retry tests set their own count.
    fn config(request_timeout_ms: u64) -> HttpClientConfig {
        HttpClientConfig {
            request_timeout: Some(Duration::from_millis(request_timeout_ms)),
            max_retries: 0,
            ..HttpClientConfig::default()
        }
    }

    fn retrying(max_retries: usize, request_timeout_ms: u64) -> HttpClientConfig {
        HttpClientConfig { max_retries, ..config(request_timeout_ms) }
    }

    // A connect timeout is retried. The listener's accept queue is full, so a
    // connection to it is never established.
    #[tokio::test]
    async fn a_connect_timeout_is_retried() {
        let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        socket.bind(&"127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap().into()).unwrap();
        socket.listen(1).unwrap();
        let address = socket.local_addr().unwrap().as_socket().unwrap();
        let mut filling = Vec::new();
        while let Ok(stream) = std::net::TcpStream::connect_timeout(&address, Duration::from_millis(200)) {
            filling.push(stream);
            assert!(filling.len() < 50, "the accept queue never filled");
        }

        let attempt = Duration::from_millis(300);
        let mut config = retrying(3, 10_000);
        config.connect_timeout = Some(attempt);
        let started = Instant::now();
        let err = HyperClient::with_config(&config)
            .post_binary_raw(&format!("http://{address}/x"), b"payload".to_vec(), &RequestHeaders::default(), false)
            .await
            .unwrap_err();

        assert!(matches!(err, KineticaError::Connection(_)), "{err}");
        assert!(started.elapsed() >= attempt * 4, "sent 1 + 3 times: {:?}", started.elapsed());
        drop(socket);
    }

    // A request that cannot be sent fails with "Error submitting endpoint
    // <url> request after <N>ms due to: <cause>", with the real cause rather
    // than hyper's "client error (Connect)".
    #[tokio::test]
    async fn a_refused_connection_is_reported_with_its_cause() {
        let (_held, address) = crate::test_net::refusing_address();
        let url = format!("http://{address}/show/system/status");
        let err = HyperClient::with_config(&retrying(0, 2_000))
            .post_binary_raw(&url, b"payload".to_vec(), &RequestHeaders::default(), false)
            .await
            .unwrap_err();
        let KineticaError::Connection(message) = err else { panic!("{err:?}") };
        let prefix = format!("Error submitting endpoint <{url}> request after <");
        assert!(message.starts_with(&prefix), "{message}");
        let (elapsed, cause) = message[prefix.len()..].split_once(">ms due to: ").expect("the message framing");
        assert!(elapsed.parse::<u64>().is_ok(), "{message}");
        assert!(cause.starts_with("Connection refused"), "{message}");
    }

    /// Reads one whole request, head and body, so the server can answer it.
    async fn read_request(stream: &mut TcpStream) -> bool {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte).await {
                Ok(1) => head.push(byte[0]),
                _ => return false,
            }
        }
        let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
        let length = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body).await.is_ok()
    }

    /// A server that answers request N with `statuses[N]`, or 200 once they
    /// run out, closing the connection after each; returns the request count.
    async fn spawn_status_server(statuses: &[u16]) -> (String, Arc<AtomicUsize>) {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let statuses = Arc::new(statuses.to_vec());
        let (url, _) = spawn_server(move |mut stream| {
            let (seen, statuses) = (seen.clone(), statuses.clone());
            async move {
                if !read_request(&mut stream).await {
                    return;
                }
                let n = seen.fetch_add(1, Ordering::SeqCst);
                let status = statuses.get(n).copied().unwrap_or(200);
                let response =
                    format!("HTTP/1.1 {status} X\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        })
        .await;
        (url, requests)
    }

    async fn post(client: &HyperClient, url: &str) -> Result<RawResponse, KineticaError> {
        client.post_binary_raw(url, b"payload".to_vec(), &RequestHeaders::default(), false).await
    }

    #[tokio::test]
    async fn a_502_or_503_is_retried_until_the_server_answers() {
        let (url, requests) = spawn_status_server(&[503, 502]).await;
        let resp = post(&HyperClient::with_config(&retrying(5, 2_000)), &url).await.expect("answered");
        assert_eq!(resp.status, 200);
        assert_eq!(requests.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn the_last_502_is_returned_once_the_retries_run_out() {
        let (url, requests) = spawn_status_server(&[502; 10]).await;
        let resp = post(&HyperClient::with_config(&retrying(2, 2_000)), &url).await.expect("answered");
        assert_eq!(resp.status, 502);
        assert_eq!(requests.load(Ordering::SeqCst), 3, "the first attempt and two retries");
    }

    #[tokio::test]
    async fn other_statuses_and_max_retries_0_are_not_retried() {
        let (url, requests) = spawn_status_server(&[500]).await;
        assert_eq!(post(&HyperClient::with_config(&retrying(5, 2_000)), &url).await.unwrap().status, 500);
        assert_eq!(requests.load(Ordering::SeqCst), 1);

        let (url, requests) = spawn_status_server(&[503]).await;
        assert_eq!(post(&HyperClient::with_config(&retrying(0, 2_000)), &url).await.unwrap().status, 503);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn the_fast_path_is_never_retried() {
        let (url, requests) = spawn_status_server(&[503, 503]).await;
        let client = HyperClient::with_config(&retrying(5, 2_000));
        let resp = client.post_binary_raw_fast(&url, b"payload".to_vec(), &RequestHeaders::default()).await.expect("answered");
        assert_eq!(resp.status, 503);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_connection_closed_without_a_response_is_retried() {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let (url, _) = spawn_server(move |mut stream| {
            let seen = seen.clone();
            async move {
                if !read_request(&mut stream).await {
                    return;
                }
                // Hang up on the first request; answer the next.
                if seen.fetch_add(1, Ordering::SeqCst) > 0 {
                    let _ = stream
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                        .await;
                }
                let _ = stream.shutdown().await;
            }
        })
        .await;

        let resp = post(&HyperClient::with_config(&retrying(5, 2_000)), &url).await.expect("answered");
        assert_eq!(resp.status, 200);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_response_timeout_is_retried() {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let (url, _) = spawn_server(move |mut stream| {
            let seen = seen.clone();
            async move {
                if read_request(&mut stream).await {
                    seen.fetch_add(1, Ordering::SeqCst);
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        })
        .await;

        let err = post(&HyperClient::with_config(&retrying(2, 150)), &url).await.expect_err("silent");
        assert!(matches!(err, KineticaError::Connection(_)), "got {err:?}");
        // The server counts a request once it has read it, which can come
        // after the client has given up on the last one
        assert!(
            crate::test_wait::eventually(|| requests.load(Ordering::SeqCst) == 3).await,
            "the first attempt and two retries, not {}",
            requests.load(Ordering::SeqCst)
        );
    }

    /// Reads one request's head, so the server can answer it.
    async fn read_request_head(stream: &mut TcpStream) -> bool {
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        while !buf.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte).await {
                Ok(1) => buf.push(byte[0]),
                _ => return false,
            }
        }
        true
    }

    /// Headers with only Basic credentials.
    fn basic_auth() -> RequestHeaders {
        RequestHeaders { authorization: Some("Basic dXNlcjpwYXNz".to_string()), ..Default::default() }
    }

    /// The headers of the request a client sends, as received, in order.
    async fn sent_headers(headers: RequestHeaders) -> Vec<String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let tx = Arc::new(parking_lot::Mutex::new(Some(tx)));
        let (url, _) = spawn_server(move |mut stream| {
            let tx = tx.clone();
            async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match stream.read(&mut byte).await {
                        Ok(1) => head.push(byte[0]),
                        _ => return,
                    }
                }
                if let Some(tx) = tx.lock().take() {
                    let _ = tx.send(head);
                }
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await;
            }
        })
        .await;
        HyperClient::with_config(&config(5_000))
            .post_binary(&url, b"payload".to_vec(), &headers)
            .await
            .expect("answered");
        let head = String::from_utf8(rx.await.unwrap()).unwrap();
        head.lines().skip(1).filter(|l| !l.is_empty()).map(str::to_lowercase).collect()
    }

    #[tokio::test]
    async fn the_servers_reason_phrase_is_kept() {
        let (url, _) = spawn_server(|mut stream| async move {
            read_request_head(&mut stream).await;
            let _ = stream.write_all(b"HTTP/1.1 503 Kinetica is exiting\r\nContent-Length: 0\r\n\r\n").await;
        })
        .await;
        let mut config = config(5_000);
        config.max_retries = 0;
        let resp = HyperClient::with_config(&config)
            .post_binary_raw(&url, b"payload".to_vec(), &RequestHeaders::default(), false)
            .await
            .expect("answered");
        assert_eq!((resp.status, resp.reason.as_str()), (503, "Kinetica is exiting"));
    }

    // The caller's headers, the session, the HA sync mode, the authorization,
    // then the content type; no `Connection` header of its own.
    #[tokio::test]
    async fn requests_carry_the_expected_headers() {
        let headers = sent_headers(RequestHeaders {
            user: vec![("X-Custom".to_string(), "1".to_string()), ("User-Agent".to_string(), "ua/1".to_string())],
            session: Some("s-1".to_string()),
            ha_sync_mode: Some("REPL_SYNC".to_string()),
            authorization: Some("Bearer t".to_string()),
        })
        .await;
        let ours: Vec<&str> = headers
            .iter()
            .map(String::as_str)
            .filter(|h| !h.starts_with("host:") && !h.starts_with("content-length:"))
            .collect();
        assert_eq!(
            ours,
            [
                "x-custom: 1",
                "user-agent: ua/1",
                "x-kinetica-session: s-1",
                "x-kinetica-group: repl_sync",
                "authorization: bearer t",
                "content-type: application/octet-stream"
            ]
        );

        let headers = sent_headers(RequestHeaders::default()).await;
        assert!(!headers.iter().any(|h| {
            h.starts_with("authorization:") || h.starts_with("x-kinetica-group:") || h.starts_with("x-kinetica-session:")
        }));
        assert!(!headers.iter().any(|h| h.starts_with("connection:")));
    }

    /// A listener that accepts connections and then runs `serve` on each one.
    async fn spawn_server<F, Fut>(serve: F) -> (String, Arc<AtomicUsize>)
    where
        F: Fn(TcpStream) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        let serve = Arc::new(serve);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve(stream));
            }
        });
        (url, accepted)
    }

    // The timeout bounds the wait for a response once the request is sent,
    // never the upload. Here the server reads nothing for a second, longer
    // than the timeout, while a body too large for the socket buffers waits
    // to go out, then reads it and answers at once.
    #[tokio::test]
    async fn an_upload_longer_than_the_timeout_is_not_cut_off() {
        const BODY: usize = 32 << 20;
        let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        socket.set_recv_buffer_size(64 << 10).unwrap();
        socket.bind(&"127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap().into()).unwrap();
        socket.listen(16).unwrap();
        socket.set_nonblocking(true).unwrap();
        let listener = TcpListener::from_std(std::net::TcpListener::from(socket)).unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request_head(&mut stream).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut body = vec![0u8; BODY];
            if stream.read_exact(&mut body).await.is_ok() {
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await;
            }
        });

        let started = Instant::now();
        let resp = HyperClient::with_config(&config(500))
            .post_binary_raw(&url, vec![7u8; BODY], &RequestHeaders::default(), false)
            .await
            .expect("sent whole");
        assert_eq!(resp.body.as_ref(), b"ok");
        assert!(started.elapsed() > Duration::from_secs(1), "the upload outlasted the timeout");
    }

    // The timeout bounds each read of the body, not the whole of it: a body
    // arriving steadily is read whole however long it takes, and one that
    // stalls times out.
    #[tokio::test]
    async fn the_timeout_bounds_each_read_of_the_body() {
        let (url, _) = spawn_server(|mut stream| async move {
            read_request(&mut stream).await;
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n").await;
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = stream.write_all(b"ab").await;
            }
        })
        .await;
        let started = Instant::now();
        let resp = HyperClient::with_config(&config(800))
            .post_binary_raw(&url, b"payload".to_vec(), &RequestHeaders::default(), false)
            .await
            .expect("read whole");
        assert_eq!(resp.body.as_ref(), b"ababababab");
        assert!(started.elapsed() > Duration::from_millis(800), "the body outlasted the timeout");

        let (url, _) = spawn_server(|mut stream| async move {
            read_request(&mut stream).await;
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nab").await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        })
        .await;
        let err = HyperClient::with_config(&config(300))
            .post_binary_raw(&url, b"payload".to_vec(), &RequestHeaders::default(), false)
            .await
            .unwrap_err();
        assert!(matches!(err, KineticaError::Http(_)), "{err}");
        assert!(err.to_string().contains("Timed out after 300 ms waiting for the response body"), "{err}");
    }

    #[tokio::test]
    async fn a_server_that_never_responds_times_out() {
        let (url, _) = spawn_server(|mut stream| async move {
            read_request_head(&mut stream).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;

        let client = HyperClient::with_config(&config(200));
        let started = Instant::now();
        let err = client.get_text(&url).await.expect_err("should time out");

        assert!(matches!(err, KineticaError::Connection(_)), "got {err:?}");
        assert!(started.elapsed() < Duration::from_secs(5), "took {:?}", started.elapsed());
    }

    #[tokio::test]
    async fn a_stalled_body_times_out() {
        let (url, _) = spawn_server(|mut stream| async move {
            read_request_head(&mut stream).await;
            // Promise a body, send part of it, then stall.
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial").await;
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;

        let client = HyperClient::with_config(&config(200));
        let err = client
            .post_binary(&url, b"payload".to_vec(), &basic_auth())
            .await
            .expect_err("should time out");

        // A body that stalls is a submit failure, not a connection failure.
        assert!(matches!(err, KineticaError::Http(ref m) if m.contains("body")), "got {err:?}");
    }

    #[tokio::test]
    async fn the_health_check_has_its_own_limit() {
        let (url, _) = spawn_server(|mut stream| async move {
            read_request_head(&mut stream).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;

        // No request timeout at all: only the explicit limit applies.
        let client = HyperClient::with_config(&HttpClientConfig {
            request_timeout: None,
            ..HttpClientConfig::default()
        });
        let err = client
            .get_text_with_timeout(&url, Duration::from_millis(200))
            .await
            .expect_err("should time out");

        assert!(matches!(err, KineticaError::Connection(_)), "got {err:?}");
    }

    // A connection is not reused past its time to live, one minute from its
    // opening; within it, it is.
    #[tokio::test]
    async fn a_connection_is_not_reused_past_its_time_to_live() {
        let (url, accepted) = spawn_server(|mut stream| async move {
            while read_request_head(&mut stream).await {
                if stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await.is_err() {
                    return;
                }
            }
        })
        .await;

        let client = HyperClient::with_config(&HttpClientConfig {
            connection_time_to_live: Duration::from_secs(1),
            ..config(5_000)
        });
        assert_eq!(client.get_text(&url).await.unwrap(), "ok");
        assert_eq!(post(&client, &url).await.unwrap().body.as_ref(), b"ok");
        assert_eq!(accepted.load(Ordering::SeqCst), 1, "reused within its time to live");

        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(post(&client, &url).await.unwrap().body.as_ref(), b"ok");
        assert_eq!(client.get_text(&url).await.unwrap(), "ok");
        assert_eq!(accepted.load(Ordering::SeqCst), 2, "a new connection once the first had lived its time");
        assert_eq!(HttpClientConfig::default().connection_time_to_live, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn sequential_requests_reuse_one_connection() {
        let (url, accepted) = spawn_server(|mut stream| async move {
            while read_request_head(&mut stream).await {
                if stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await.is_err() {
                    return;
                }
            }
        })
        .await;

        let client = HyperClient::with_config(&config(5_000));
        for _ in 0..5 {
            assert_eq!(client.get_text(&url).await.unwrap(), "ok");
        }

        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "idle connections must be kept for reuse, not closed after each request"
        );
    }

    /// A server that answers each request after `delay`. Servers given the
    /// same `in_flight` counter share it, so `peak` records the most requests
    /// they were handling at the same moment.
    async fn spawn_slow_server(delay: Duration, in_flight: Arc<AtomicUsize>, peak: Arc<AtomicUsize>) -> String {
        let (url, _) = spawn_server(move |mut stream| {
            let (in_flight, peak) = (in_flight.clone(), peak.clone());
            async move {
                while read_request_head(&mut stream).await {
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(delay).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    if stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await.is_err() {
                        return;
                    }
                }
            }
        })
        .await;
        url
    }

    fn limited(per_host: usize, total: usize) -> HttpClientConfig {
        HttpClientConfig {
            max_connections_per_host: per_host,
            max_total_connections: total,
            ..config(10_000)
        }
    }

    async fn fire(client: &HyperClient, urls: &[String], each: usize) {
        let requests = urls.iter().flat_map(|u| std::iter::repeat_n(u.clone(), each)).map(|u| {
            let client = client.clone();
            tokio::spawn(async move { client.get_text(&u).await })
        });
        for r in futures_join(requests.collect()).await {
            assert_eq!(r.unwrap().unwrap(), "ok");
        }
    }

    async fn futures_join<T>(handles: Vec<tokio::task::JoinHandle<T>>) -> Vec<Result<T, tokio::task::JoinError>> {
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await);
        }
        out
    }

    #[tokio::test]
    async fn requests_to_one_host_are_capped() {
        let peak = Arc::new(AtomicUsize::new(0));
        let url = spawn_slow_server(Duration::from_millis(100), Arc::default(), peak.clone()).await;

        let client = HyperClient::with_config(&limited(2, 40));
        fire(&client, &[url], 6).await;

        assert_eq!(peak.load(Ordering::SeqCst), 2, "at most max_connections_per_host in flight");
    }

    #[tokio::test]
    async fn requests_across_hosts_are_capped_in_total() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let a = spawn_slow_server(Duration::from_millis(100), in_flight.clone(), peak.clone()).await;
        let b = spawn_slow_server(Duration::from_millis(100), in_flight, peak.clone()).await;

        // Each host may take 3, but only 3 in total.
        let client = HyperClient::with_config(&limited(3, 3));
        fire(&client, &[a, b], 4).await;

        assert_eq!(peak.load(Ordering::SeqCst), 3, "at most max_total_connections in flight at once");
    }

    #[tokio::test]
    async fn waiting_too_long_for_a_connection_is_a_connection_error() {
        let (url, _) = spawn_server(|mut stream| async move {
            read_request_head(&mut stream).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;

        let client = HyperClient::with_config(&HttpClientConfig {
            connection_request_timeout: Duration::from_millis(200),
            ..limited(1, 40)
        });

        // Occupy the only connection to the host.
        let busy = client.clone();
        let hog_url = url.clone();
        let hog = tokio::spawn(async move { busy.get_text(&hog_url).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let err = client.get_text(&url).await.expect_err("no connection should come free");
        assert!(matches!(err, KineticaError::Connection(ref m) if m.contains("free connection")), "got {err:?}");
        hog.abort();
    }

    #[tokio::test]
    async fn a_saturated_pool_does_not_block_health_checks() {
        let (url, _) = spawn_server(|mut stream| async move {
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                if stream.read(&mut byte).await.unwrap_or(0) != 1 {
                    return;
                }
                head.push(byte[0]);
            }
            if head.starts_with(b"GET /slow") {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await;
        })
        .await;

        let client = HyperClient::with_config(&limited(1, 1));
        let busy = client.clone();
        let slow_url = format!("{url}slow");
        let hog = tokio::spawn(async move { busy.get_text(&slow_url).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let pong = client.get_text_with_timeout(&url, Duration::from_secs(2)).await;
        assert_eq!(pong.expect("health check has its own pool"), "ok");
        hog.abort();
    }

    #[tokio::test]
    async fn an_error_status_is_reported_with_one_prefix() {
        let (url, _) = spawn_server(|mut stream| async move {
            read_request_head(&mut stream).await;
            let _ = stream
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 4\r\n\r\nboom")
                .await;
        })
        .await;

        let client = HyperClient::with_config(&config(5_000));
        let err = client
            .post_binary(&url, b"payload".to_vec(), &basic_auth())
            .await
            .expect_err("a 500 is an error");

        assert_eq!(err.to_string(), "HTTP error: 500 Internal Server Error - boom");
    }

    #[test]
    fn options_map_onto_the_client_config() {
        let mut options = crate::KineticaOptions::default();
        options
            .set_timeout(1_500)
            .set_server_connection_timeout(700)
            .set_max_connections_per_host(3)
            .set_bypass_ssl_cert_check(true);

        let config = HttpClientConfig::from_options(&options);
        assert_eq!(config.request_timeout, Some(Duration::from_millis(1_500)));
        assert_eq!(config.connect_timeout, Some(Duration::from_millis(700)));
        assert_eq!(config.max_connections_per_host, 3);
        assert!(config.bypass_ssl_cert_check);
    }

    #[test]
    fn a_zero_timeout_means_no_limit() {
        let mut options = crate::KineticaOptions::default();
        options.set_timeout(0).set_server_connection_timeout(0);

        let config = HttpClientConfig::from_options(&options);
        assert_eq!(config.request_timeout, None);
        assert_eq!(config.connect_timeout, None);
    }

    #[test]
    fn defaults_match_the_documented_values() {
        let config = HttpClientConfig::default();
        assert_eq!(config.request_timeout, None, "no request timeout by default");
        assert_eq!(config.connect_timeout, Some(Duration::from_millis(5_000)));
        assert_eq!(config.max_retries, 5, "five retries by default");
    }
}

#[cfg(test)]
mod authorization_tests {
    use super::authorization_header;
    use super::base64::decode_mime;

    // MIME base64: anything outside the alphabet is skipped, and a malformed
    // ending fails.
    #[test]
    fn base64_is_decoded_as_mime() {
        assert_eq!(decode_mime("dXNlcjpwYXNz").unwrap(), b"user:pass");
        assert_eq!(decode_mime("dXNl\r\ncjpw YXNz").unwrap(), b"user:pass");
        assert_eq!(decode_mime("YWI6Yw==").unwrap(), b"ab:c");
        assert_eq!(decode_mime("YWI6Y2Q=").unwrap(), b"ab:cd");
        assert_eq!(decode_mime("YWI6Yw").unwrap(), b"ab:c", "padding may be left out");
        assert_eq!(decode_mime("YWI6Yw=!").unwrap_err(), "Base64 padding '=' is misplaced");
        assert_eq!(decode_mime("YWI6Y").unwrap_err(), "Base64 text ends with a lone character");
        assert_eq!(decode_mime("YWI6Yw==QQ").unwrap_err(), "Base64 text continues after its padding, at character 9");
        assert_eq!(decode_mime("YWI6Yw== !").unwrap(), b"ab:c");
    }

    #[test]
    fn an_oauth_token_wins_over_credentials() {
        assert_eq!(authorization_header("tok", "user", "pass").as_deref(), Some("Bearer tok"));
    }

    #[test]
    fn credentials_are_sent_as_basic_when_either_is_set() {
        assert_eq!(authorization_header("", "user", "pass").as_deref(), Some("Basic dXNlcjpwYXNz"));
        assert_eq!(authorization_header("", "user", "").as_deref(), Some("Basic dXNlcjo="));
        assert_eq!(authorization_header("", "", "pass").as_deref(), Some("Basic OnBhc3M="));
    }

    #[test]
    fn no_header_is_sent_without_a_token_or_credentials() {
        // No header at all; this used to send "Basic Og==".
        assert_eq!(authorization_header("", "", ""), None);
    }

    #[test]
    fn credentials_are_base64_of_their_utf8_without_line_breaks() {
        // Each length of final group: 0, 1 and 2 bytes left over.
        assert_eq!(authorization_header("", "ab", "c").as_deref(), Some("Basic YWI6Yw=="));
        assert_eq!(authorization_header("", "ab", "cd").as_deref(), Some("Basic YWI6Y2Q="));
        assert_eq!(authorization_header("", "ab", "cde").as_deref(), Some("Basic YWI6Y2Rl"));
        assert_eq!(authorization_header("", "ü", "ß").as_deref(), Some("Basic w7w6w58="));
        let long = authorization_header("", &"u".repeat(60), &"p".repeat(60)).unwrap();
        assert!(!long.contains('\n') && !long.contains('\r'));
    }
}
