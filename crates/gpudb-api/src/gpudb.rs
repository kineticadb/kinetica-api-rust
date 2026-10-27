//! Core Kinetica client implementation.
//!
//! This module provides the main `Kinetica` struct which handles
//! communication with the Kinetica database server.

use crate::{KineticaError, KineticaRecord, hyper_client::{HttpClientConfig, HyperClient}};
use log::{debug, error, warn, trace};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_avro_fast::Schema as FastSchema;
use std::str::FromStr;
use parking_lot::RwLock;
use std::sync::{Arc, OnceLock};
use std::collections::{HashMap, HashSet, VecDeque};
use url::Url;
use rand::seq::SliceRandom;

// Global cached wrapper schema to avoid parsing on every request
static WRAPPER_SCHEMA: OnceLock<FastSchema> = OnceLock::new();

/// Kinetica API response wrapper structure
#[derive(serde::Deserialize, Debug)]
struct WrapperResponse {
    status: String,
    message: String,
    #[allow(dead_code)]
    schema: String,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

// Default port for host manager URLs
pub const DEFAULT_HOST_MANAGER_PORT: u16 = 9300;

/// The largest initial connection attempt timeout, in milliseconds: its
/// nanoseconds must fit an `i64`.
const MAX_INITIAL_CONNECTION_ATTEMPT_TIMEOUT_MS: u64 = 9_223_372_036_854;

/// Largest payload Snappy is asked to compress. Anything above this is sent
/// uncompressed rather than erroring.
pub const MAX_SNAPPY_COMPRESSION_SIZE: usize = 1_840_700_241;

/// Key of the response `info` entry by which the server tells the client it
/// routed the request's data somewhere other than where the client's shard
/// mapping said it should go — that is, shard rebalancing is under way and the
/// client's mapping is stale. The value is the string `"true"` when set.
pub(crate) const RESPONSE_INFO_DATA_REROUTED: &str = "data_rerouted";

/// Whether a response's `info` map carries the data-rerouted flag, meaning the
/// client's shard mapping is stale and should be refreshed.
pub(crate) fn is_data_rerouted(info: &HashMap<String, String>) -> bool {
    info.get(RESPONSE_INFO_DATA_REROUTED)
        .is_some_and(|v| v == "true")
}

// Default timeout values (in milliseconds)
const DEFAULT_SERVER_CONNECTION_TIMEOUT: u64 = 5000;

/// Limit on a liveness ping.
const DEFAULT_INITIAL_CONNECTION_ATTEMPT_TIMEOUT_MS: u64 = 0;


/// High availability failover order options.
///
/// The default is `Sequential`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HaFailoverOrder {
    /// Failover to clusters in a random order
    Random,
    /// Failover to clusters in sequential order (default)
    #[default]
    Sequential,
}

/// The high availability synchronicity override sent with each request.
/// Anything but `Default` is sent as the `X-Kinetica-Group` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HaSynchronicityMode {
    /// No override; the HA process decides which endpoints are synchronous.
    #[default]
    Default,
    /// Do not replicate the endpoint calls.
    None,
    /// Execute the query locally, then send it to each cluster in turn.
    Synchronous,
    /// Send the query to each cluster and execute it locally, at once.
    SynchronousParallel,
    /// Execute the query locally, then queue it for each cluster.
    Asynchronous,
    /// Queue the query for each cluster and execute it locally, at once.
    AsynchronousParallel,
}

impl HaSynchronicityMode {
    /// Every mode; [`Self::mode`] gives each one's name as the server takes it.
    pub const ALL: [HaSynchronicityMode; 6] = [
        Self::Default,
        Self::None,
        Self::Synchronous,
        Self::SynchronousParallel,
        Self::Asynchronous,
        Self::AsynchronousParallel,
    ];

    /// The mode as the server names it.
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::None => "REPL_NONE",
            Self::Synchronous => "REPL_SYNC",
            Self::SynchronousParallel => "REPL_SYNC_PARALLEL",
            Self::Asynchronous => "REPL_ASYNC",
            Self::AsynchronousParallel => "REPL_ASYNC_PARALLEL",
        }
    }

    /// The mode's constant name, such as `SYNCHRONOUS_PARALLEL`.
    fn name(&self) -> &'static str {
        match self {
            Self::Default => "DEFAULT",
            Self::None => "NONE",
            Self::Synchronous => "SYNCHRONOUS",
            Self::SynchronousParallel => "SYNCHRONOUS_PARALLEL",
            Self::Asynchronous => "ASYNCHRONOUS",
            Self::AsynchronousParallel => "ASYNCHRONOUS_PARALLEL",
        }
    }

    /// The mode named by `value`, in any case, as a mode, as a constant
    /// name, or as a mode without its `REPL_` prefix.
    pub fn from_value(value: &str) -> Option<Self> {
        let upper = value.to_uppercase();
        let upper_plus = format!("REPL_{upper}");
        Self::ALL
            .into_iter()
            .find(|mode| mode.mode() == upper || mode.name() == upper || mode.mode() == upper_plus)
    }

    /// The mode an `X-Kinetica-Group` header in the options names, read as
    /// [`Self::from_value`] reads one: the value the header carries on the
    /// wire (`REPL_SYNC`) as well as the constant name (`SYNCHRONOUS`), in
    /// any case. A value that names no mode fails.
    fn from_header(value: &str) -> Result<Self, KineticaError> {
        Self::from_value(value).ok_or_else(|| {
            KineticaError::Config(format!("Unknown HA synchronicity mode: {value}"))
        })
    }
}

/// The error of a request that failed on every cluster it could fail over
/// to: the original failure, described with its kind, then why there is
/// nowhere left to go.
pub(crate) fn failover_exhausted(error: &KineticaError, ha_message: &str) -> KineticaError {
    KineticaError::Connection(format!("{}; {ha_message}", error.cause_description()))
}

/// The headers the client sets itself, which a caller cannot set, change or
/// remove.
const HEADER_HA_SYNC_MODE: &str = "X-Kinetica-Group";
const HEADER_AUTHORIZATION: &str = "Authorization";
const HEADER_CONTENT_TYPE: &str = "Content-type";
const HEADER_USER_AGENT: &str = "User-Agent";
const HEADER_SESSION: &str = "X-Kinetica-Session";
const PROTECTED_HEADERS: [&str; 5] =
    [HEADER_HA_SYNC_MODE, HEADER_AUTHORIZATION, HEADER_CONTENT_TYPE, HEADER_USER_AGENT, HEADER_SESSION];

/// The key in a response's `info` map that the server sets to `"true"` once
/// this connection's session owns objects (SQL `#name` temp tables) that
/// [`Kinetica::close`] must drop.
const RESPONSE_INFO_SESSION_ID_USED: &str = "session_id_used";

/// How long the drop of a session for a connection dropped without being
/// closed waits.
const SESSION_DROP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The program name reported in the `User-Agent` header sent with every
/// request.
pub const USER_AGENT_PROGRAM_NAME: &str = "kinetica-api-rust";

/// Replaces every character an HTTP `User-Agent` token cannot hold (RFC 7230
/// `tchar`) with an underscore.
fn sanitize_user_agent_token(value: &str) -> String {
    if value.is_empty() {
        return "unknown".to_string();
    }
    value
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || "!#$%&'*+.^_`|~-".contains(c) { c } else { '_' })
        .collect()
}

/// The `User-Agent` sent with every request, naming this client, the Rust
/// compiler and the platform:
///
/// ```text
/// MyApp/1.0 kinetica-api-rust/7.2.3.0 (Rust/1.88.0; Linux/6.8.0-146-generic; amd64)
/// ```
///
/// The client program's name and version, if given, come first.
fn build_user_agent(client_name: Option<&str>, client_version: Option<&str>) -> String {
    // The API version, without the build number
    let api_version = sanitize_user_agent_token(env!("GPUDB_API_VERSION"));
    let os_name = match std::env::consts::OS {
        "linux" => "Linux",
        "macos" => "Mac OS X",
        "windows" => "Windows",
        "freebsd" => "FreeBSD",
        other => other,
    };
    // The OS version is the kernel release
    let os_version = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|v| v.trim().to_string())
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    let os_arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        other => other,
    };

    let mut user_agent = String::new();
    if let Some(name) = client_name.filter(|n| !n.is_empty()) {
        user_agent.push_str(&sanitize_user_agent_token(name));
        if let Some(version) = client_version.filter(|v| !v.is_empty()) {
            user_agent.push('/');
            user_agent.push_str(&sanitize_user_agent_token(version));
        }
        user_agent.push(' ');
    }
    user_agent.push_str(&format!(
        "{USER_AGENT_PROGRAM_NAME}/{api_version} (Rust/{}; {os_name}/{os_version}; {os_arch})",
        env!("GPUDB_RUSTC_VERSION")
    ));
    user_agent
}

/// The credentials to use: those of a `Basic` `Authorization` header among
/// the caller's headers, if there is one, else the options'. A `Bearer`
/// header there is not used.
fn credentials_from_http_headers(
    headers: &HashMap<String, String>,
    username: &str,
    password: &str,
) -> Result<(String, String), KineticaError> {
    if let Some(header) = headers.get(HEADER_AUTHORIZATION).filter(|h| h.starts_with("Basic")) {
        let splits = split_dropping_trailing_empty(header, " ");
        if splits.len() == 2 && !splits[1].is_empty() {
            let decoded = crate::hyper_client::base64::decode_mime(splits[1]).map_err(KineticaError::Config)?;
            let credentials = String::from_utf8_lossy(&decoded).into_owned();
            let user_password = split_dropping_trailing_empty(&credentials, ":");
            let Some(password) = user_password.get(1) else {
                return Err(KineticaError::Config(
                    "The Basic authorization header's credentials have no ':' between user name and password"
                        .to_string(),
                ));
            };
            return Ok((user_password[0].to_string(), password.to_string()));
        }
    }
    Ok((username.to_string(), password.to_string()))
}

/// Extended configuration options for the Kinetica client.
///
/// This struct provides comprehensive configuration for the Kinetica client
/// including authentication, connection settings, and HA configuration.
///
/// Large batches are encoded and decoded on rayon's threads, with the same
/// results as one thread gives (see [`crate::avro`]). The connection pool
/// checks every connection before it hands one out, and `timeout` applies
/// to every request.
#[derive(Clone)]
pub struct KineticaOptions {
    /// URL of the primary cluster in the HA environment
    pub primary_url: String,

    /// Username for authentication
    pub username: String,

    /// Password for authentication
    pub password: String,

    /// OAuth2 token for authentication. When set, it is sent as a `Bearer`
    /// token in place of the username and password.
    pub oauth_token: String,

    /// Regex pattern to filter URLs by hostname/IP
    pub hostname_regex: Option<String>,

    /// Whether to Snappy-compress requests to the endpoints that carry bulk
    /// record payloads — `/insert/records` and `/update/records`.
    ///
    /// This is a switch over an opt-in set, not a global one: every other
    /// endpoint is sent uncompressed whatever this is set to, because
    /// compressing a small request costs more than it saves.
    pub use_snappy: bool,

    /// Whether to bypass SSL certificate verification
    pub bypass_ssl_cert_check: bool,

    /// The trust store HTTPS connections trust: a PKCS#12 or JKS file, or a
    /// PEM file of certificates. Used when its password is given too and
    /// certificates are checked. A server whose own certificate is in it, or
    /// whose chain leads to one in it, is trusted, and its name is not
    /// checked.
    pub trust_store_file_path: String,

    /// The password of the trust store.
    pub trust_store_password: String,

    /// Whether to disable failover upon failures
    pub disable_failover: bool,

    /// Whether to disable automatic discovery of clusters and worker ranks
    pub disable_auto_discovery: bool,

    /// Order in which to failover to backup clusters
    pub ha_failover_order: HaFailoverOrder,

    /// Additional HTTP headers sent with each request.
    ///
    /// The headers the client sets itself (`Authorization`, `Content-type`,
    /// `User-Agent` and `X-Kinetica-Group`) are not sent from here. `Basic`
    /// credentials in an `Authorization` header are used in place of the
    /// username and password, and an `X-Kinetica-Group` header names the HA
    /// synchronicity mode, by its value on the wire or its
    /// [`HaSynchronicityMode`] constant name (`REPL_SYNC` or `SYNCHRONOUS`).
    pub http_headers: HashMap<String, String>,

    /// The client program's name, reported first in the `User-Agent` header.
    pub client_name: Option<String>,

    /// The client program's version, reported with its name.
    pub client_version: Option<String>,

    /// The server session the connection's SQL `#name` temp tables belong
    /// to, so that several connections can share one session's temp tables.
    ///
    /// The caller must drop that session, with [`Kinetica::drop_session`] on
    /// a connection given the same session ID; [`Kinetica::close`] does not.
    /// If `None` or empty (the default), each connection has a session of its
    /// own, which its `close` drops.
    pub session_id: Option<String>,

    /// Whether connections send TCP keep-alive probes, with the operating
    /// system's timings unless set below (default `true`).
    pub tcp_keep_alive: bool,

    /// Seconds a connection idles before the first keep-alive probe; 0 (the
    /// default) keeps the system's, typically 7200 on Linux.
    pub tcp_keep_idle: u32,

    /// Seconds between keep-alive probes; 0 (the default) keeps the
    /// system's, typically 75 on Linux.
    pub tcp_keep_interval: u32,

    /// Keep-alive probes before a connection is dropped; 0 (the default)
    /// keeps the system's, typically 9 on Linux.
    pub tcp_keep_count: u32,

    /// Request timeout in milliseconds (0 = infinite).
    ///
    /// A read timeout: it bounds the wait for a response once the request has
    /// been sent, and then each read of the response body, never the upload
    /// or the body as a whole. So a large request or response that keeps
    /// moving is not cut off, while a server that goes quiet for this long is
    /// given up on. A timeout waiting for a response is treated like an
    /// unreachable server and triggers HA failover.
    pub timeout: u64,

    /// Timeout for establishing a connection, in milliseconds (0 = infinite)
    pub server_connection_timeout: u64,

    /// Host manager port number
    pub host_manager_port: u16,

    /// Initial connection attempt timeout in milliseconds (0 = no retry)
    pub initial_connection_attempt_timeout: u64,

    /// Maximum number of requests in flight across all hosts (default 40). At
    /// least 1.
    ///
    /// A request beyond the limit waits up to 5 minutes for a free connection
    /// and then fails as a connection error. High-concurrency callers such as
    /// a `BulkInserter` with many workers should raise this to match.
    pub max_total_connections: usize,

    /// Maximum number of requests in flight to one host (default 10), and the
    /// number of idle connections kept per host for reuse. At least 1.
    ///
    /// A request beyond the limit waits as for `max_total_connections`. A
    /// `BulkInserter` with more than this many flush workers sending to one
    /// host is throttled to this many concurrent requests.
    pub max_connections_per_host: usize,

    /// How many times a request is retried at once, on the same server, when
    /// sending it fails or the server answers 502 or 503 (default 5); see
    /// [`HttpClientConfig::max_retries`](crate::hyper_client::HttpClientConfig::max_retries).
    pub max_retries: usize,

    /// How often, in seconds, to check whether the primary cluster is back
    /// after an HA failover has moved the connection off it (default 5). Once
    /// it passes, the connection fails back to it; see the `failback` module.
    pub failback_polling_interval: u64,
}

/// The default [`KineticaOptions::max_retries`].
const DEFAULT_HTTP_REQUEST_MAX_RETRIES: usize = 5;

/// The default [`KineticaOptions::failback_polling_interval`], in seconds.
const DEFAULT_FAILBACK_POLLING_INTERVAL: u64 = 5;

/// Prints the options with the password masked: `<********>`, or `""` when
/// it is empty.
impl std::fmt::Debug for KineticaOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct Masked<'a>(&'a str);
        impl std::fmt::Debug for Masked<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(if self.0.is_empty() { "\"\"" } else { "<********>" })
            }
        }

        f.debug_struct("KineticaOptions")
            .field("primary_url", &self.primary_url)
            .field("username", &self.username)
            .field("password", &Masked(&self.password))
            .field("oauth_token", &Masked(&self.oauth_token))
            .field("hostname_regex", &self.hostname_regex)
            .field("use_snappy", &self.use_snappy)
            .field("trust_store_file_path", &self.trust_store_file_path)
            .field("trust_store_password", &Masked(&self.trust_store_password))
            .field("bypass_ssl_cert_check", &self.bypass_ssl_cert_check)
            .field("disable_failover", &self.disable_failover)
            .field("disable_auto_discovery", &self.disable_auto_discovery)
            .field("ha_failover_order", &self.ha_failover_order)
            .field("http_headers", &self.http_headers)
            .field("client_name", &self.client_name)
            .field("client_version", &self.client_version)
            .field("session_id", &self.session_id)
            .field("timeout", &self.timeout)
            .field("tcp_keep_alive", &self.tcp_keep_alive)
            .field("tcp_keep_idle", &self.tcp_keep_idle)
            .field("tcp_keep_interval", &self.tcp_keep_interval)
            .field("tcp_keep_count", &self.tcp_keep_count)
            .field("server_connection_timeout", &self.server_connection_timeout)
            .field("host_manager_port", &self.host_manager_port)
            .field("initial_connection_attempt_timeout", &self.initial_connection_attempt_timeout)
            .field("max_total_connections", &self.max_total_connections)
            .field("max_connections_per_host", &self.max_connections_per_host)
            .field("max_retries", &self.max_retries)
            .field("failback_polling_interval", &self.failback_polling_interval)
            .finish()
    }
}

impl Default for KineticaOptions {
    fn default() -> Self {
        Self {
            primary_url: String::new(),
            username: String::new(),
            password: String::new(),
            oauth_token: String::new(),
            hostname_regex: None,
            use_snappy: true,
            bypass_ssl_cert_check: false,
            trust_store_file_path: String::new(),
            trust_store_password: String::new(),
            disable_failover: false,
            disable_auto_discovery: false,
            ha_failover_order: HaFailoverOrder::default(),
            http_headers: HashMap::new(),
            client_name: None,
            client_version: None,
            session_id: None,
            tcp_keep_alive: true,
            tcp_keep_idle: 0,
            tcp_keep_interval: 0,
            tcp_keep_count: 0,
            timeout: 0,
            server_connection_timeout: DEFAULT_SERVER_CONNECTION_TIMEOUT,
            host_manager_port: DEFAULT_HOST_MANAGER_PORT,
            initial_connection_attempt_timeout: DEFAULT_INITIAL_CONNECTION_ATTEMPT_TIMEOUT_MS,
            max_total_connections: 40,
            max_connections_per_host: 10,
            max_retries: DEFAULT_HTTP_REQUEST_MAX_RETRIES,
            failback_polling_interval: DEFAULT_FAILBACK_POLLING_INTERVAL,
        }
    }
}

impl KineticaOptions {
    /// Creates a new KineticaOptions with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the primary URL. Returns self for method chaining.
    pub fn set_primary_url(&mut self, value: impl Into<String>) -> &mut Self {
        self.primary_url = value.into();
        self
    }

    /// Sets the username. Returns self for method chaining.
    pub fn set_username(&mut self, value: impl Into<String>) -> &mut Self {
        self.username = value.into();
        self
    }

    /// Sets the password. Returns self for method chaining.
    pub fn set_password(&mut self, value: impl Into<String>) -> &mut Self {
        self.password = value.into();
        self
    }

    /// Sets the OAuth2 token. Returns self for method chaining.
    pub fn set_oauth_token(&mut self, value: impl Into<String>) -> &mut Self {
        self.oauth_token = value.into();
        self
    }

    /// Replaces the additional HTTP headers sent with each request; see
    /// [`KineticaOptions::http_headers`]. Returns self for method chaining.
    pub fn set_http_headers(&mut self, value: HashMap<String, String>) -> &mut Self {
        self.http_headers = value;
        self
    }

    /// Adds an HTTP header sent with each request, replacing one of the same
    /// name; see [`KineticaOptions::http_headers`]. Returns self for method
    /// chaining.
    pub fn add_http_header(&mut self, header: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.http_headers.insert(header.into(), value.into());
        self
    }

    /// Sets the client program's name and version, reported first in the
    /// `User-Agent` header. A `None` name clears both. Returns self for method
    /// chaining.
    pub fn set_client_info(&mut self, client_name: Option<&str>, client_version: Option<&str>) -> &mut Self {
        match client_name {
            Some(name) => {
                self.client_name = Some(name.to_string());
                self.client_version = client_version.map(str::to_string);
            }
            None => {
                self.client_name = None;
                self.client_version = None;
            }
        }
        self
    }

    /// Sets the server session to share; see [`KineticaOptions::session_id`].
    /// `None` gives each connection a session of its own. Returns self for
    /// method chaining.
    pub fn set_session_id(&mut self, value: Option<&str>) -> &mut Self {
        self.session_id = value.map(str::to_string);
        self
    }

    /// Sets whether connections send TCP keep-alive probes; see
    /// [`KineticaOptions::tcp_keep_alive`]. Returns self for method chaining.
    pub fn set_tcp_keep_alive(&mut self, value: bool) -> &mut Self {
        self.tcp_keep_alive = value;
        self
    }

    /// Sets the seconds a connection idles before the first keep-alive probe,
    /// 0 for the system's. Ignored, with a warning, when keep-alive is off.
    /// Returns self for method chaining.
    pub fn set_tcp_keep_idle(&mut self, seconds: u32) -> &mut Self {
        self.tcp_keep_idle = seconds;
        self
    }

    /// Sets the seconds between keep-alive probes, 0 for the system's.
    /// Ignored, with a warning, when keep-alive is off. Returns self for
    /// method chaining.
    pub fn set_tcp_keep_interval(&mut self, seconds: u32) -> &mut Self {
        self.tcp_keep_interval = seconds;
        self
    }

    /// Sets the keep-alive probes before a connection is dropped, 0 for the
    /// system's. Ignored, with a warning, when keep-alive is off. Returns
    /// self for method chaining.
    pub fn set_tcp_keep_count(&mut self, count: u32) -> &mut Self {
        self.tcp_keep_count = count;
        self
    }

    /// Sets the hostname regex. Returns self for method chaining.
    pub fn set_hostname_regex(&mut self, value: Option<String>) -> &mut Self {
        self.hostname_regex = value;
        self
    }

    /// Sets whether to Snappy-compress bulk record requests (`/insert/records`
    /// and `/update/records`). Returns self for method chaining.
    ///
    /// See [`KineticaOptions::use_snappy`] — other endpoints are unaffected.
    pub fn set_use_snappy(&mut self, value: bool) -> &mut Self {
        self.use_snappy = value;
        self
    }

    /// Sets whether to bypass SSL certificate check. Returns self for method chaining.
    pub fn set_bypass_ssl_cert_check(&mut self, value: bool) -> &mut Self {
        self.bypass_ssl_cert_check = value;
        self
    }

    /// Sets the trust store file. See
    /// [`KineticaOptions::trust_store_file_path`].
    pub fn set_trust_store_file_path(&mut self, value: impl Into<String>) -> &mut Self {
        self.trust_store_file_path = value.into();
        self
    }

    /// Sets the trust store's password.
    pub fn set_trust_store_password(&mut self, value: impl Into<String>) -> &mut Self {
        self.trust_store_password = value.into();
        self
    }

    /// The trust store file.
    pub fn trust_store_file_path(&self) -> &str {
        &self.trust_store_file_path
    }

    /// The trust store's password.
    pub fn trust_store_password(&self) -> &str {
        &self.trust_store_password
    }

    /// Sets whether to disable failover. Returns self for method chaining.
    pub fn set_disable_failover(&mut self, value: bool) -> &mut Self {
        self.disable_failover = value;
        self
    }

    /// Sets whether to disable auto-discovery. Returns self for method chaining.
    pub fn set_disable_auto_discovery(&mut self, value: bool) -> &mut Self {
        self.disable_auto_discovery = value;
        self
    }

    /// Sets the HA failover order. Returns self for method chaining.
    pub fn set_ha_failover_order(&mut self, value: HaFailoverOrder) -> &mut Self {
        self.ha_failover_order = value;
        self
    }

    /// Sets the request timeout in milliseconds. Returns self for method chaining.
    pub fn set_timeout(&mut self, value: u64) -> &mut Self {
        self.timeout = value;
        self
    }

    /// Sets the server connection timeout in milliseconds. Returns self for method chaining.
    pub fn set_server_connection_timeout(&mut self, value: u64) -> &mut Self {
        self.server_connection_timeout = value;
        self
    }

    /// Sets the host manager port. Returns self for method chaining.
    ///
    /// A host manager address the server publishes for the head node's host
    /// (`conf.host<i>_host_manager_public_url`) is used over any port, and
    /// while this is left at [`DEFAULT_HOST_MANAGER_PORT`] the server's
    /// reported `conf.hm_http_port` is used instead of it.
    pub fn set_host_manager_port(&mut self, value: u16) -> &mut Self {
        self.host_manager_port = value;
        self
    }

    /// Sets the initial connection attempt timeout in milliseconds. Returns self for method chaining.
    pub fn set_initial_connection_attempt_timeout(&mut self, value: u64) -> &mut Self {
        self.initial_connection_attempt_timeout = value;
        self
    }

    /// Sets how often, in seconds, to poll the primary cluster for fail-back.
    /// Returns self for method chaining.
    pub fn set_failback_polling_interval(&mut self, seconds: u64) -> &mut Self {
        self.failback_polling_interval = seconds;
        self
    }

    /// Sets the maximum total connections. Returns self for method chaining.
    pub fn set_max_total_connections(&mut self, value: usize) -> &mut Self {
        self.max_total_connections = value;
        self
    }

    /// Sets the maximum connections per host. Returns self for method chaining.
    pub fn set_max_connections_per_host(&mut self, value: usize) -> &mut Self {
        self.max_connections_per_host = value;
        self
    }

    /// Sets the maximum number of HTTP retries. Returns self for method chaining.
    pub fn set_max_retries(&mut self, value: usize) -> &mut Self {
        self.max_retries = value;
        self
    }
}

// ============================================================================
// Server version
// ============================================================================

/// The version of a Kinetica server.
///
/// Versions compare component by component, major first and the build last,
/// so two versions are equal only when their builds are too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KineticaVersion {
    major: i32,
    minor: i32,
    revision: i32,
    abi_version: i32,
    build: i64,
}

impl KineticaVersion {
    /// A version with a build of 0.
    pub const fn new(major: i32, minor: i32, revision: i32, abi_version: i32) -> Self {
        Self::with_build(major, minor, revision, abi_version, 0)
    }

    /// A version with the given build.
    pub const fn with_build(major: i32, minor: i32, revision: i32, abi_version: i32, build: i64) -> Self {
        Self { major, minor, revision, abi_version, build }
    }

    pub fn major(&self) -> i32 {
        self.major
    }

    pub fn minor(&self) -> i32 {
        self.minor
    }

    pub fn revision(&self) -> i32 {
        self.revision
    }

    pub fn abi_version(&self) -> i32 {
        self.abi_version
    }

    pub fn build(&self) -> i64 {
        self.build
    }

    /// `major.minor.revision.abi_version`.
    pub fn version(&self) -> String {
        format!("{}.{}.{}.{}", self.major, self.minor, self.revision, self.abi_version)
    }

    /// The version followed by the build.
    pub fn full_version(&self) -> String {
        format!("{}.{}", self.version(), self.build)
    }
}

impl std::fmt::Display for KineticaVersion {
    /// The version without the build.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.version())
    }
}

/// The system property that holds the server's version.
const DATABASE_SERVER_VERSION_KEY: &str = "version.gpudb_core_version";

/// Reads the server version from `/show/system/properties`: `A.B.C.D.build`,
/// split on the first four periods, each of the first four an `i32` and the
/// rest an `i64`.
///
/// A version without a build fails, on the missing fifth component.
fn parse_server_version(
    system_properties: &HashMap<String, String>,
) -> Result<KineticaVersion, KineticaError> {
    let Some(version) = system_properties.get(DATABASE_SERVER_VERSION_KEY) else {
        return Err(KineticaError::Api(format!(
            "System properties does not have any entry for the '{DATABASE_SERVER_VERSION_KEY}' key"
        )));
    };

    let parse = || -> Result<KineticaVersion, String> {
        let components: Vec<&str> = version.splitn(5, '.').collect();
        if components.len() < 4 {
            return Err(format!(
                "Server version string in /show/system/properties response malformed (expected at \
                 least four components): {version}"
            ));
        }
        let int = |s: &str| s.parse::<i32>().map_err(|_| format!("'{s}' is not an i32 version component"));
        let major = int(components[0])?;
        let minor = int(components[1])?;
        let revision = int(components[2])?;
        let abi_version = int(components[3])?;
        let build_text = components
            .get(4)
            .ok_or_else(|| "no build number after the four version components".to_string())?;
        let build = build_text
            .parse::<i64>()
            .map_err(|_| format!("'{build_text}' is not an i64 build number"))?;
        Ok(KineticaVersion::with_build(major, minor, revision, abi_version, build))
    };

    parse().map_err(|e| KineticaError::Api(format!("Could not parse server version; error: {e}")))
}

use crate::address_resolution::{self as resolution, split_dropping_trailing_empty, HostnameRegex};

/// A list of URLs, for messages: `[http://a:9191/, http://b:9191/]`.
fn url_list(urls: &[Url]) -> String {
    let shown: Vec<&str> = urls.iter().map(Url::as_str).collect();
    format!("[{}]", shown.join(", "))
}

/// A cluster's multi-head addresses and whether multi-head is usable there,
/// as one value.
///
/// **Immutable, and replaced rather than edited.** Its identity is the change
/// signal: multi-head objects decide whether to rebuild by comparing the
/// snapshot they last built from with the current one by pointer
/// ([`Arc::ptr_eq`]). That works only because a probe publishes a *new* one, so
/// leaving a cluster and returning to it shows as a change. For the same reason
/// every cluster has its own, never one shared between clusters.
#[derive(Debug)]
pub(crate) struct MultiHeadSnapshot {
    worker_rank_urls: Vec<Option<Url>>,
    multi_head_available: bool,
}

impl MultiHeadSnapshot {
    pub(crate) fn new(worker_rank_urls: Vec<Option<Url>>, multi_head_available: bool) -> Self {
        Self { worker_rank_urls, multi_head_available }
    }

    pub(crate) fn worker_rank_urls(&self) -> &[Option<Url>] {
        &self.worker_rank_urls
    }

    pub(crate) fn is_multi_head_available(&self) -> bool {
        self.multi_head_available
    }
}

/// Contains address information for a Kinetica cluster.
///
/// This struct stores information about a cluster including its active head node URL,
/// worker rank URLs, hostnames, host manager URL, and system properties.
#[derive(Debug, Clone)]
pub struct ClusterAddressInfo {
    /// The active head node URL for the cluster
    active_head_node_url: Url,
    /// System properties map from the cluster
    system_properties: HashMap<String, String>,
    /// The worker rank URLs and the multi-head verdict, as one value; see
    /// [`MultiHeadSnapshot`] and [`ClusterAddressInfo::worker_rank_urls`].
    multi_head_snapshot: Arc<MultiHeadSnapshot>,
    /// Whether this cluster still owes a capability probe for the client's
    /// current visit to it; see [`ClusterAddressInfo::is_probe_owed`].
    probe_owed: bool,
    /// Set of hostnames/IP addresses in the cluster
    host_names: HashSet<String>,
    /// URL for the host manager
    host_manager_url: Url,
    /// Whether this is the primary cluster
    is_primary_cluster: bool,
    /// The cluster's HA drained status ("drained", "draining", "not_drained"),
    /// as last reported; see [`ClusterAddressInfo::ha_status`].
    ha_status: Option<String>,
}

impl ClusterAddressInfo {
    /// Creates a new ClusterAddressInfo for an active cluster.
    ///
    /// # Arguments
    ///
    /// * `active_head_node_url` - The URL of the active head node
    /// * `system_properties` - System properties map from the cluster
    /// * `worker_rank_urls` - Worker rank URLs, with `None` at the slot of every
    ///   rank removed from the cluster
    /// * `host_names` - Set of hostnames/IP addresses in the cluster
    /// * `host_manager_url` - URL for the host manager
    ///
    /// # Returns
    ///
    /// A new ClusterAddressInfo instance with hostnames updated based on rank URLs.
    pub fn new(
        active_head_node_url: Url,
        system_properties: HashMap<String, String>,
        worker_rank_urls: Vec<Option<Url>>,
        host_names: HashSet<String>,
        host_manager_url: Url,
    ) -> Self {
        let mut info = Self {
            active_head_node_url,
            system_properties,
            multi_head_snapshot: Arc::new(MultiHeadSnapshot::new(worker_rank_urls, true)),
            probe_owed: true,
            host_names,
            host_manager_url,
            is_primary_cluster: false,
            ha_status: None,
        };
        // Ensure that all the known ranks' hostnames are also accounted for
        info.update_hostnames_based_on_rank_urls();
        info
    }

    /// Creates a ClusterAddressInfo with just an active URL and host manager port.
    ///
    /// # Arguments
    ///
    /// * `active_head_node_url` - The URL of the active head node
    /// * `host_manager_port` - The port for the host manager
    ///
    /// # Returns
    ///
    /// A Result containing the new ClusterAddressInfo or a KineticaError.
    pub fn with_host_manager_port(
        active_head_node_url: Url,
        host_manager_port: u16,
    ) -> Result<Self, KineticaError> {
        // Create a host manager URL with the given port for host managers
        let mut host_manager_url = active_head_node_url.clone();
        if active_head_node_url.path() != "/" {
            // Behind httpd the host manager is proxied on the head node's OWN
            // port, beside it in the path; the host manager port is not
            // involved. Httpd is inferred from the path alone here, where the
            // discovery path also consults conf.enable_httpd_proxy.
            // A lone `/` is no path, as in build_host_manager_url.
            host_manager_url.set_path(&resolution::host_manager_path_for(&active_head_node_url));
        } else {
            // The host manager URL shouldn't use any path and use the host manager port
            host_manager_url
                .set_port(Some(host_manager_port))
                .map_err(|_| KineticaError::Api("Error creating the host manager URL".to_string()))?;
            host_manager_url.set_path("");
        }
        host_manager_url.set_query(None);
        host_manager_url.set_fragment(None);
        debug!("Created host manager URL: {host_manager_url}");

        let mut info = Self {
            active_head_node_url,
            system_properties: HashMap::new(),
            // No addresses, and therefore no claim to multi-head: this is the
            // bare cluster -- down at discovery, its properties unreadable, or
            // built with auto-discovery off -- so nothing is known about its
            // workers. The first probe sets the real answer.
            multi_head_snapshot: Arc::new(MultiHeadSnapshot::new(Vec::new(), false)),
            probe_owed: true,
            host_names: HashSet::new(),
            host_manager_url,
            is_primary_cluster: false,
            ha_status: None,
        };
        // Ensure that all the known ranks' hostnames are also accounted for
        info.update_hostnames_based_on_rank_urls();
        Ok(info)
    }

    /// Creates a ClusterAddressInfo with just an active URL using the default host manager port.
    ///
    /// # Arguments
    ///
    /// * `active_head_node_url` - The URL of the active head node
    ///
    /// # Returns
    ///
    /// A Result containing the new ClusterAddressInfo or a KineticaError.
    pub fn from_url(active_head_node_url: Url) -> Result<Self, KineticaError> {
        Self::with_host_manager_port(active_head_node_url, DEFAULT_HOST_MANAGER_PORT)
    }

    // Getters
    // -------

    /// Returns the active head node URL.
    pub fn active_head_node_url(&self) -> &Url {
        &self.active_head_node_url
    }

    /// Returns the system properties map.
    pub fn system_properties(&self) -> &HashMap<String, String> {
        &self.system_properties
    }

    /// Returns the worker rank URLs, where entry `i` is rank `i + 1`.
    ///
    /// A rank removed from the cluster keeps its slot, as `None`, so that these
    /// indices stay aligned with the rank numbering used by the shard routing
    /// table. Callers walking this list must skip the empty slots; to iterate
    /// only reachable ranks, use [`Self::live_worker_rank_urls`].
    pub fn worker_rank_urls(&self) -> &[Option<Url>] {
        self.multi_head_snapshot.worker_rank_urls()
    }

    /// Returns the URLs of the worker ranks still present in the cluster,
    /// skipping the empty slots left by removed ranks.
    ///
    /// Use this for health checks and hostname collection — anything that
    /// visits ranks without caring which rank number each one is. Never use it
    /// for routing: it is compacted, so its indices are not rank numbers.
    pub fn live_worker_rank_urls(&self) -> impl Iterator<Item = &Url> {
        self.worker_rank_urls().iter().flatten()
    }

    /// Returns the set of hostnames.
    pub fn host_names(&self) -> &HashSet<String> {
        &self.host_names
    }

    /// Returns the host manager URL.
    pub fn host_manager_url(&self) -> &Url {
        &self.host_manager_url
    }

    /// Returns whether this is the primary cluster.
    pub fn is_primary_cluster(&self) -> bool {
        self.is_primary_cluster
    }

    /// Whether multi-head operations are available on *this* cluster from
    /// *this* client.
    ///
    /// Distinct from the `disable_auto_discovery` option, which records what
    /// the user asked for and applies to the whole HA ring. This records what
    /// was actually observed for one cluster: a cluster whose worker addresses
    /// this client cannot reach is still usable through its head node, and that
    /// loss of capability belongs to the cluster, not to the connection.
    pub fn is_multi_head_available(&self) -> bool {
        self.multi_head_snapshot.is_multi_head_available()
    }

    /// The multi-head addresses and verdict as one value, for change detection
    /// by identity.
    pub(crate) fn multi_head_snapshot(&self) -> &Arc<MultiHeadSnapshot> {
        &self.multi_head_snapshot
    }

    /// Publishes new multi-head addresses and verdict as one value.
    pub(crate) fn set_multi_head_snapshot(&mut self, value: Arc<MultiHeadSnapshot>) {
        self.multi_head_snapshot = value;
    }

    /// Whether this cluster still owes a capability probe for the client's
    /// current visit to it: set when the connection arrives on the cluster,
    /// and cleared by the probe that runs for that arrival.
    ///
    /// It exists because the probe runs outside the switch lock, so a caller
    /// that piggybacked on another's switch can ask for addresses before that
    /// switch's probe has published any; this makes it wait for the probe
    /// instead of reading what the last visit left behind.
    pub(crate) fn is_probe_owed(&self) -> bool {
        self.probe_owed
    }

    pub(crate) fn set_probe_owed(&mut self, value: bool) {
        self.probe_owed = value;
    }

    /// The HA drained status ("drained", "draining", "not_drained") the cluster
    /// last reported, or `None` if it did not report one.
    pub fn ha_status(&self) -> Option<&str> {
        self.ha_status.as_deref()
    }

    // Setters (mutable reference pattern)
    // -------

    /// Sets the active head node URL. Returns self for method chaining.
    pub fn set_active_head_node_url(&mut self, value: Url) -> &mut Self {
        self.active_head_node_url = value;
        self
    }

    /// Sets the system properties. Returns self for method chaining.
    pub fn set_system_properties(&mut self, value: HashMap<String, String>) -> &mut Self {
        self.system_properties = value;
        self
    }

    /// Sets the worker rank URLs. Returns self for method chaining.
    ///
    /// `None` marks the slot of a rank removed from the cluster; see
    /// [`Self::worker_rank_urls`] for why those slots must be kept.
    pub fn set_worker_rank_urls(&mut self, value: Vec<Option<Url>>) -> &mut Self {
        // Carries the current verdict onto a new snapshot. The probe sets both
        // halves at once instead, through set_multi_head_snapshot.
        let available = self.is_multi_head_available();
        self.multi_head_snapshot = Arc::new(MultiHeadSnapshot::new(value, available));
        self
    }

    /// Sets the list of host names. Returns self for method chaining.
    pub fn set_host_names(&mut self, value: HashSet<String>) -> &mut Self {
        self.host_names = value;
        self
    }

    /// Sets the host manager URL. Returns self for method chaining.
    pub fn set_host_manager_url(&mut self, value: Url) -> &mut Self {
        self.host_manager_url = value;
        self
    }

    /// Sets whether this cluster is the primary one. Returns self for method chaining.
    pub fn set_primary_cluster(&mut self, value: bool) -> &mut Self {
        self.is_primary_cluster = value;
        self
    }

    /// Records whether multi-head operations are available on this cluster; see
    /// [`ClusterAddressInfo::is_multi_head_available`].
    pub fn set_multi_head_available(&mut self, value: bool) -> &mut Self {
        let urls = self.worker_rank_urls().to_vec();
        self.multi_head_snapshot = Arc::new(MultiHeadSnapshot::new(urls, value));
        self
    }

    /// Sets the HA drained status. Returns self for method chaining.
    pub fn set_ha_status(&mut self, value: Option<String>) -> &mut Self {
        self.ha_status = value;
        self
    }

    // Private Helper Methods
    // ----------------------

    /// Adds the hostnames of the head and worker rank URLs to the list of hostnames
    /// if they are not already part of it.
    fn update_hostnames_based_on_rank_urls(&mut self) {
        // Put the head rank's hostname in the saved hostnames (only if
        // it doesn't exist there already)
        if let Some(head_host) = self.active_head_node_url.host_str() {
            if !self.does_cluster_contain_node(head_host) {
                self.host_names.insert(head_host.to_string());
            }
        }

        // Put each worker rank's hostname in the saved hostnames (only if it
        // doesn't exist there already). Removed ranks have no URL and so no
        // hostname to contribute; `live_worker_rank_urls` skips their slots.
        let worker_hosts: Vec<String> = self
            .live_worker_rank_urls()
            .filter_map(|url| url.host_str().map(|s| s.to_string()))
            .collect();

        for worker_host in worker_hosts {
            if !self.does_cluster_contain_node(&worker_host) {
                self.host_names.insert(worker_host);
            }
        }
    }

    // Convenience Methods
    // --------------------

    /// Checks if the given hostname (or IP address) is part of this cluster.
    ///
    /// # Arguments
    ///
    /// * `host_name` - The hostname or IP address to check
    ///
    /// # Returns
    ///
    /// true if this cluster contains a machine with the given hostname or IP address,
    /// false otherwise.
    pub fn does_cluster_contain_node(&self, host_name: &str) -> bool {
        self.host_names.contains(host_name)
    }
}

impl PartialEq for ClusterAddressInfo {
    fn eq(&self, other: &Self) -> bool {
        self.active_head_node_url == other.active_head_node_url
            && self.worker_rank_urls() == other.worker_rank_urls()
            && self.host_names == other.host_names
            && self.is_primary_cluster == other.is_primary_cluster
    }
}

impl Eq for ClusterAddressInfo {}

impl std::hash::Hash for ClusterAddressInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.active_head_node_url.hash(state);
        // Hash worker_rank_urls in order
        for url in self.worker_rank_urls() {
            url.hash(state);
        }
        // Hash host_names - need to sort for consistent hashing
        let mut host_names: Vec<_> = self.host_names.iter().collect();
        host_names.sort();
        for name in host_names {
            name.hash(state);
        }
        self.host_manager_url.hash(state);
        self.is_primary_cluster.hash(state);
    }
}

impl std::fmt::Display for ClusterAddressInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format worker rank URLs as simple strings, showing removed ranks as
        // the empty slots they are rather than hiding them.
        let worker_urls: Vec<&str> = self
            .worker_rank_urls()
            .iter()
            .map(|url| url.as_ref().map_or("<removed>", |u| u.as_str()))
            .collect();
        write!(
            f,
            "{{ active_head_node_url: {}, worker_rank_urls: {:?}, host_names: {:?}, host_manager_url: {}, is_primary_cluster: {} }}",
            self.active_head_node_url.as_str(),
            worker_urls,
            self.host_names,
            self.host_manager_url.as_str(),
            self.is_primary_cluster
        )
    }
}

fn wrapper_schema() -> &'static FastSchema {
    WRAPPER_SCHEMA.get_or_init(|| {
        let wrapper_schema_json = r#"{"type":"record","name":"res","fields":[{"name":"status","type":"string"},{"name":"message","type":"string"},{"name":"schema","type":"string"},{"name":"data","type":"bytes"}]}"#;
        FastSchema::from_str(wrapper_schema_json).expect("Failed to parse wrapper schema")
    })
}

// Server messages that mean the cluster cannot serve requests, and so should
// trigger a failover.
const DB_OFFLINE_ERROR_MESSAGE: &str = "Kinetica is offline";
const DB_CONNECTION_RESET_ERROR_MESSAGE: &str = "Connection reset";
const DB_CONNECTION_REFUSED_ERROR_MESSAGE: &str = "Connection refused";
const DB_EXITING_ERROR_MESSAGE: &str = "Kinetica is exiting";
const DB_SHUTTING_DOWN_ERROR_MESSAGE: &str = "System shutting down";
const DB_DRAINING_HAQ_ERROR_MESSAGE: &str = "Unavailable: Draining HA queue";
const DB_SYSTEM_LIMITED_ERROR_MESSAGE: &str = "system-limited-fatal";
const DB_QP_DOWN_ERROR_MESSAGE: &str = "Query planner is not running";
/// A server error saying the server sent no response, which is taken as an
/// exit.
const DB_EOF_FROM_SERVER_ERROR_MESSAGE: &str = "Unexpected end of file from server";

/// Interprets a response, returning its wrapper or the error it amounts to.
///
/// The error kind decides what happens next. [`KineticaError::Exit`] and
/// [`KineticaError::Http`] fail over to another cluster;
/// [`KineticaError::Unauthorized`] and [`KineticaError::Api`] do not.
///
/// - 401 or 403 is unauthorized.
/// - A text body (an HTML page from a proxy, say) is an exit on 500, 502, 503
///   or 504, and a submit failure otherwise.
/// - An empty body is an exit.
/// - An Avro `ERROR` is an exit if the status is 500, 502, 503 or 504, or if
///   the message says the server is exiting, offline, shutting down or
///   draining; any other `ERROR` is an API error.
/// - A body that is not a valid response is a submit failure.
fn interpret_response(url: &str, resp: &crate::hyper_client::RawResponse) -> Result<WrapperResponse, KineticaError> {
    let (status, reason) = (resp.status, resp.reason.as_str());

    if status == 401 || status == 403 {
        return Err(KineticaError::Unauthorized(format!("Server response status: {status} : {reason}")));
    }

    let gateway_or_server_error = matches!(status, 500 | 502 | 503 | 504);

    if resp.content_type.as_deref().is_some_and(|ct| ct.starts_with("text")) {
        if gateway_or_server_error {
            debug!("Returning an exit error from {url}; response_code: {status}; response message: {reason}");
            return Err(KineticaError::Exit(reason.to_string()));
        }
        return Err(KineticaError::Http(format!(
            "Cannot parse response from server: '{reason}'; status code: {status}"
        )));
    }

    if resp.body.is_empty() {
        return Err(KineticaError::Exit(format!(
            "Server returned HTTP {status} ({reason}) with an empty body"
        )));
    }

    // A body that does not decode is a submit failure, which fails over; note
    // that this includes a response this client cannot read.
    let wrapper: WrapperResponse = serde_avro_fast::from_datum_slice(&resp.body, wrapper_schema())
        .map_err(|e| KineticaError::Http(format!("Wrapper decode failed: {e}")))?;
    let message = wrapper.message.as_str();

    if status == 400 {
        if message.contains(DB_SHUTTING_DOWN_ERROR_MESSAGE) {
            return Err(KineticaError::Exit("Kinetica shutting down".to_string()));
        }
        if message.contains(DB_DRAINING_HAQ_ERROR_MESSAGE) {
            return Err(KineticaError::Exit("Draining HA message queue".to_string()));
        }
    }

    if wrapper.status == "ERROR" {
        let server_unavailable = [
            DB_EXITING_ERROR_MESSAGE,
            DB_CONNECTION_REFUSED_ERROR_MESSAGE,
            DB_CONNECTION_RESET_ERROR_MESSAGE,
            DB_SYSTEM_LIMITED_ERROR_MESSAGE,
            DB_OFFLINE_ERROR_MESSAGE,
            DB_QP_DOWN_ERROR_MESSAGE,
        ]
        .iter()
        .any(|m| message.contains(m));

        if status == 500 || status == 504 || server_unavailable {
            debug!("Returning an exit error from {url}; response_code: {status}; message: {message}");
            return Err(KineticaError::Exit(message.to_string()));
        }
        if status == 502 || status == 503 {
            debug!("Returning an exit error from {url}; response_code: {status}; message: {message}");
            return Err(KineticaError::Exit(format!("Cluster may be stopped or suspended: {message}")));
        }
        // A legitimate error, unless it says the server did not send a
        // response, which is an exit and fails over.
        debug!("Returning an error from {url}; response_code: {status}; message: {message}");
        if message.contains(DB_EOF_FROM_SERVER_ERROR_MESSAGE) {
            return Err(KineticaError::Exit(message.to_string()));
        }
        return Err(KineticaError::Api(message.to_string()));
    }

    Ok(wrapper)
}

/// The index into `host_addresses` of the cluster at position `pointer` of
/// the failover order `indices`, or `None` where there is none: a ring of one
/// cluster is always at 0.
pub(crate) fn cluster_slot(host_addresses: &[ClusterAddressInfo], indices: &[usize], pointer: usize) -> Option<usize> {
    if host_addresses.is_empty() {
        return None;
    }
    if host_addresses.len() == 1 {
        return Some(0);
    }
    indices.get(pointer).copied().filter(|&i| i < host_addresses.len())
}

/// Parses `spec` as an absolute `http` or `https` URL. Fails for text with
/// no scheme and for any other scheme -- so `myhost:9191`, whose `myhost`
/// the url crate takes for a scheme, is no URL -- and with the url crate's
/// account of any other fault.
pub(crate) fn parse_absolute_url(spec: &str) -> Result<Url, String> {
    match Url::parse(spec) {
        Ok(url) if matches!(url.scheme(), "http" | "https") => Ok(url),
        Ok(url) => Err(format!("unsupported URL scheme '{}'; use http or https", url.scheme())),
        Err(url::ParseError::RelativeUrlWithoutBase) => Err(format!("'{spec}' has no URL scheme")),
        Err(e) => Err(e.to_string()),
    }
}

/// `url` with `path` appended to its path, as an endpoint's address is
/// built: from the scheme, host, port and path alone, so any user
/// information, query or fragment the URL carries is dropped, and with one
/// slash between the two where the URL's path ends in one. A query in `path`
/// (a JSON endpoint's parameters) is kept.
pub(crate) fn append_path_to_url(url: &Url, path: &str) -> Url {
    let (path, query) = match path.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path, None),
    };
    let base = url.path();
    let joined = match (base.ends_with('/'), path.strip_prefix('/')) {
        (true, Some(rest)) => format!("{base}{rest}"),
        _ => format!("{base}{path}"),
    };
    let mut appended = url.clone();
    appended.set_path(&joined);
    appended.set_query(query);
    appended.set_fragment(None);
    // Only fails for a URL that cannot have a host, which these always have
    let _ = appended.set_username("");
    let _ = appended.set_password(None);
    appended
}

/// The headers every request carries: the caller's, the User-Agent among
/// them, the HA synchronicity override, and the authorization.
pub(crate) struct HeaderState {
    authorization: Option<String>,
    http_headers: RwLock<HashMap<String, String>>,
    ha_sync_mode: RwLock<HaSynchronicityMode>,
    /// The connection's session ID, sent as `X-Kinetica-Session`.
    session_id: Option<String>,
}

/// No headers: no authorization, no caller's headers, the default mode, no
/// session.
impl Default for HeaderState {
    fn default() -> Self {
        HeaderState {
            authorization: None,
            http_headers: RwLock::new(HashMap::new()),
            ha_sync_mode: RwLock::new(HaSynchronicityMode::Default),
            session_id: None,
        }
    }
}

impl HeaderState {
    /// The headers to send with a request now.
    pub(crate) fn request_headers(&self) -> crate::hyper_client::RequestHeaders {
        let mode = *self.ha_sync_mode.read();
        crate::hyper_client::RequestHeaders {
            user: self.http_headers.read().iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            session: self.session_id.clone(),
            ha_sync_mode: (mode != HaSynchronicityMode::Default).then(|| mode.mode().to_string()),
            authorization: self.authorization.clone(),
        }
    }
}

/// This connection's server session, which owns the SQL `#name` temp tables
/// it creates.
///
/// Every request carries the session's ID. Once a response reports the
/// session used, the drop of a session of the connection's own is prepared,
/// for [`Kinetica::close`] to send, or failing that for the connection's
/// `Drop`. A session the caller gave ([`KineticaOptions::session_id`]) is
/// never dropped here: it is the caller's to drop.
pub(crate) struct Session {
    id: String,
    /// Whether the session is the connection's own, rather than the caller's.
    owned: bool,
    /// The request that drops the session, prepared when the server first
    /// reports the session used; `None` until then, and once it is spent.
    armed: parking_lot::Mutex<Option<ArmedDrop>>,
}

/// A prepared drop of a session, so that it can be sent without the
/// connection.
struct ArmedDrop {
    url: String,
    headers: crate::hyper_client::RequestHeaders,
    body: hyper::body::Bytes,
}

impl Session {
    /// The session named by `id`, the caller's; or, given `None` or an empty
    /// ID, a new session of the connection's own.
    fn new(id: Option<&str>) -> Self {
        let armed = parking_lot::Mutex::new(None);
        match id.filter(|id| !id.is_empty()) {
            Some(id) => Self { id: id.to_string(), owned: false, armed },
            None => Self { id: uuid::Uuid::new_v4().to_string(), owned: true, armed },
        }
    }

    fn is_armed(&self) -> bool {
        self.armed.lock().is_some()
    }

    fn arm(&self, drop: ArmedDrop) {
        *self.armed.lock() = Some(drop);
    }

    /// Takes the prepared drop, leaving the session disarmed.
    fn disarm(&self) -> Option<ArmedDrop> {
        self.armed.lock().take()
    }
}

/// Whether a response's `info` map reports the session used. `data` is the
/// response payload, decoded against `resp_schema` once already.
///
/// Every response would otherwise be decoded twice, since the typed one has
/// no common way to reach its `info`. So the payload is first searched for
/// the key's bytes, which an Avro string holds as they are; only a payload
/// carrying them is decoded again, for its `info` map alone.
fn reports_session_used(data: &[u8], resp_schema: &FastSchema) -> bool {
    if memchr::memmem::find(data, RESPONSE_INFO_SESSION_ID_USED.as_bytes()).is_none() {
        return false;
    }

    #[derive(serde::Deserialize)]
    struct InfoOnly {
        #[serde(default)]
        info: Option<HashMap<String, String>>,
    }
    serde_avro_fast::from_datum_slice::<InfoOnly>(data, resp_schema)
        .ok()
        .and_then(|response| response.info)
        .is_some_and(|info| info.get(RESPONSE_INFO_SESSION_ID_USED).map(String::as_str) == Some("true"))
}

/// What `url` answers a GET with, or an empty string if it does not answer,
/// within `timeout_ms` (0 for no limit).
///
/// The ping goes through the connection's own client, with its trust store
/// or certificate bypass.
pub(crate) async fn ping_url(client: &HyperClient, headers: &HeaderState, url: &Url, timeout_ms: u64) -> String {
    debug!("Pinging URL: {url}");
    let limit = (timeout_ms > 0).then(|| std::time::Duration::from_millis(timeout_ms));
    client
        .ping(url.as_str(), &headers.request_headers(), limit)
        .await
        .unwrap_or_default()
}

/// The timeout, in milliseconds, of the ping that checks Kinetica is running.
const RUNNING_CHECK_TIMEOUT_MS: u64 = 5000;

/// Whether Kinetica answers at `url`, without failing over. Shared by
/// [`Kinetica::is_kinetica_running`] and the fail-back poller, which pings
/// with the connection's headers.
pub(crate) async fn is_kinetica_running_at(client: &HyperClient, headers: &HeaderState, url: &Url) -> bool {
    let response = ping_url(client, headers, url, RUNNING_CHECK_TIMEOUT_MS).await;
    debug!("HTTP server @ {url} responded with: '{response}'");
    response == "Kinetica is running!"
}

/// The main Kinetica client for communicating with a Kinetica database.
///
/// This client is thread-safe and can be shared across tasks using `Arc<Kinetica>`.
///
/// # Performance
///
/// - Uses connection pooling for efficient reuse of HTTP connections
/// - Enables TCP_NODELAY for lower latency
/// - Supports parallel requests via async/await
///
/// # Example
///
/// ```no_run
/// use gpudb_api::{Kinetica, KineticaOptions};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut options = KineticaOptions::new();
/// options.set_username("admin")
///        .set_password("password")
///        .set_use_snappy(true);
///
/// let kinetica = Arc::new(
///     Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
/// );
/// # Ok(())
/// # }
/// ```
pub struct Kinetica {
    /// Base URL of the Kinetica server, as given at construction: the head
    /// node to fall back to when no cluster information is known.
    ///
    /// Not updated by an HA failover or fail-back; [`Kinetica::url`] gives
    /// the cluster currently in use.
    pub(crate) url: String,

    /// Configuration options (kept for public API access).
    ///
    /// Public, password included; its `Debug` output masks the password.
    pub options: KineticaOptions,

    /// High-performance hyper-based HTTP client for all operations
    hyper_client: HyperClient,

    /// List of cluster address information objects. Shared, as is the
    /// failover order, the pointer into it and the switch lock, with the
    /// fail-back poller, which switches the connection back to the primary
    /// itself.
    host_addresses: Arc<RwLock<Vec<ClusterAddressInfo>>>,

    /// The hostname of the primary URL
    primary_url_hostname: RwLock<String>,

    /// List of HA URL indices for failover (randomized or sequential)
    ha_url_indices: Arc<RwLock<Vec<usize>>>,

    /// Current cluster index pointer for failover
    current_cluster_index_pointer: Arc<RwLock<usize>>,

    /// Number of times we've switched clusters
    num_cluster_switches: RwLock<usize>,

    /// Username for authentication
    username: String,

    /// Password for authentication
    password: String,

    /// The headers sent with every request, shared with the fail-back
    /// poller, which pings with them.
    headers: Arc<HeaderState>,

    /// The client program's name and version, from the options.
    client_name: Option<String>,
    client_version: Option<String>,

    /// Hostname regex pattern for filtering URLs
    hostname_regex: Option<HostnameRegex>,

    /// Whether to Snappy-compress requests to the bulk record endpoints; see
    /// [`KineticaOptions::use_snappy`].
    use_snappy: bool,

    /// Whether a payload too large for Snappy has been warned about; later
    /// ones are logged at debug.
    did_snappy_compression_size_warning: std::sync::atomic::AtomicBool,

    /// Whether failover is disabled
    disable_failover: bool,

    /// Whether auto-discovery is disabled
    disable_auto_discovery: RwLock<bool>,

    /// Host manager port
    host_manager_port: u16,

    /// Initial connection attempt timeout in nanoseconds
    initial_connection_attempt_timeout_ns: u64,

    /// High availability failover order
    ha_failover_order: HaFailoverOrder,

    /// The fail-back poller, while one is running; see the `failback` module.
    failback_poller: parking_lot::Mutex<Option<crate::failback::FailbackPoller>>,

    /// The version of the server connected to, read when the client is built.
    server_version: Option<KineticaVersion>,

    /// The record types known by type ID, used to decode records when no type
    /// is given. See the `known_types` module.
    pub(crate) known_types: parking_lot::RwLock<HashMap<String, Arc<crate::generic_record::Type>>>,

    /// Serializes cluster switches: `switch_url` holds it while it reads and
    /// advances the cluster index. Asynchronous because the lock is held
    /// across the health check of each candidate cluster.
    switch_lock: Arc<tokio::sync::Mutex<()>>,

    /// Serializes [`Kinetica::probe_current_cluster_multi_head`]: one probe at
    /// a time publishes a cluster's addresses and verdict, and a caller
    /// waiting out an arrival's probe waits here. Not `switch_lock`, which
    /// would block failover for the length of a probe's network I/O.
    probe_lock: tokio::sync::Mutex<()>,

    /// This connection's server session; see [`Session`].
    session: Session,

    /// Whether the connection was built, after which dropping it without
    /// closing it drops its session.
    session_cleanup_registered: bool,

    /// Set by [`Kinetica::close`].
    closed: std::sync::atomic::AtomicBool,
}

impl Kinetica {
    /// Creates a new Kinetica client instance with full initialization.
    ///
    /// This constructor performs the full initialization sequence:
    /// - Parses and validates URLs
    /// - Discovers cluster information (unless auto-discovery is disabled)
    /// - Sets up HA failover configuration
    /// - Checks connectivity: fails over from a first cluster that is not
    ///   usable, then reads the server version from the cluster selected, and
    ///   fails if either cannot be done
    ///
    /// # Arguments
    ///
    /// * `urls` - List of URLs to connect to (e.g., `vec!["http://localhost:9191".to_string()]`)
    /// * `options` - Configuration options including credentials and connection settings
    ///
    /// # Returns
    ///
    /// Returns a fully initialized `Kinetica` instance or an error if initialization fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::{Kinetica, KineticaOptions};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut options = KineticaOptions::new();
    /// options.set_username("admin")
    ///        .set_password("password")
    ///        .set_use_snappy(true);
    ///
    /// let kinetica = Arc::new(
    ///     Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(urls: Vec<String>, options: KineticaOptions) -> Result<Self, KineticaError> {
        let mut kinetica = Self::connect(urls, options).await?;
        kinetica.check_connectivity().await?;
        Ok(kinetica)
    }

    /// Verifies the connection, once the URLs have been processed.
    ///
    /// With failover and auto-discovery on, a first cluster that is not usable
    /// (down, or draining its HA queue) is failed over from, by the same test
    /// runtime failover uses. Then the system properties are read from the
    /// cluster selected and the server version recorded. Any failure fails the
    /// construction: "Connectivity check for <url> failed: ...".
    async fn check_connectivity(&mut self) -> Result<(), KineticaError> {
        let checked = async {
            let url = self.url().ok_or_else(|| KineticaError::Connection("No URL available".to_string()))?;
            if !self.disable_failover
                && !*self.disable_auto_discovery.read()
                && !self.is_cluster_usable(&url, false).await
            {
                debug!("Initially selected cluster <{url}> is not usable (down or draining); failing over");
                self.switch_url(&url, self.num_cluster_switches()).await?;
            }

            let url = self.url().ok_or_else(|| KineticaError::Connection("No URL available".to_string()))?;
            let system_properties = self.get_system_properties_at_url(&url).await?;
            parse_server_version(&system_properties)
        }
        .await;

        match checked {
            Ok(version) => {
                self.server_version = Some(version);
                Ok(())
            }
            Err(e) => {
                let url = self.url().map(|u| u.to_string()).unwrap_or_default();
                Err(KineticaError::caused(format!("Connectivity check for <{url}> failed: {e}"), e))
            }
        }
    }

    /// Gets the version of the server this client is connected to.
    pub fn server_version(&self) -> Option<&KineticaVersion> {
        self.server_version.as_ref()
    }

    /// The options this client was created with.
    pub fn options(&self) -> &KineticaOptions {
        &self.options
    }

    /// The request timeout, in milliseconds (0 for none).
    pub fn timeout(&self) -> u64 {
        self.options.timeout
    }

    /// How many times a request is retried on the same server.
    pub fn max_retries(&self) -> usize {
        self.options.max_retries
    }

    /// The username requests authenticate with, from the options or from a
    /// `Basic` `Authorization` header among them.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The password requests authenticate with, from the options or from a
    /// `Basic` `Authorization` header among them.
    pub fn password(&self) -> &str {
        &self.password
    }

    /// Whether bulk record requests are Snappy-compressed.
    pub fn use_snappy(&self) -> bool {
        self.use_snappy
    }

    /// Whether server certificates go unchecked.
    pub fn bypass_ssl_cert_check(&self) -> bool {
        self.options.bypass_ssl_cert_check
    }

    /// The URL of the primary cluster, if one is identified as primary.
    pub fn primary_url(&self) -> Option<Url> {
        if self.primary_url_hostname.read().is_empty() {
            return None;
        }
        self.host_addresses.read().first().map(|c| c.active_head_node_url().clone())
    }

    /// The host name or IP address of the primary cluster, or an empty
    /// string if no cluster is identified as primary.
    pub fn primary_hostname(&self) -> String {
        self.primary_url_hostname.read().clone()
    }

    /// The system properties of the cluster in use.
    pub fn system_properties(&self) -> HashMap<String, String> {
        self.cluster_info().map(|c| c.system_properties().clone()).unwrap_or_default()
    }

    /// This client's version and build number: the Kinetica API version
    /// (MAJOR.MINOR.REVISION.ABI, the crate version `7.2.3-0` as `7.2.3.0`),
    /// then the time of the build, `7.2.3.0-20261010120000`.
    pub fn api_version() -> &'static str {
        env!("GPUDB_API_VERSION_FULL")
    }

    /// The additional HTTP headers sent with each request, the `User-Agent`
    /// among them.
    pub fn http_headers(&self) -> HashMap<String, String> {
        self.headers.http_headers.read().clone()
    }

    /// Adds an HTTP header sent with each request, replacing one of the same
    /// name. The headers the client sets itself (`Authorization`,
    /// `Content-type`, `User-Agent`, `X-Kinetica-Group`, `X-Kinetica-Session`)
    /// cannot be changed.
    pub fn add_http_header(&self, header: &str, value: &str) -> Result<(), KineticaError> {
        if PROTECTED_HEADERS.contains(&header) {
            let message = format!("Not allowed to change protected header: {header}");
            error!("{message}");
            return Err(KineticaError::Api(message));
        }
        self.headers.http_headers.write().insert(header.to_string(), value.to_string());
        Ok(())
    }

    /// Stops sending an HTTP header. The headers the client sets itself
    /// cannot be removed.
    pub fn remove_http_header(&self, header: &str) -> Result<(), KineticaError> {
        if PROTECTED_HEADERS.contains(&header) {
            let message = format!("Not allowed to remove protected header: {header}");
            error!("{message}");
            return Err(KineticaError::Api(message));
        }
        self.headers.http_headers.write().remove(header);
        Ok(())
    }

    /// The `User-Agent` header sent with each request.
    pub fn user_agent(&self) -> Option<String> {
        self.headers.http_headers.read().get(HEADER_USER_AGENT).cloned()
    }

    /// The client program's name, from the options.
    pub fn client_name(&self) -> Option<&str> {
        self.client_name.as_deref()
    }

    /// The client program's version, from the options.
    pub fn client_version(&self) -> Option<&str> {
        self.client_version.as_deref()
    }

    /// Sets the HA synchronicity override sent with each subsequent request.
    pub fn set_ha_sync_mode(&self, mode: HaSynchronicityMode) {
        *self.headers.ha_sync_mode.write() = mode;
    }

    /// The HA synchronicity override sent with each request.
    pub fn ha_sync_mode(&self) -> HaSynchronicityMode {
        *self.headers.ha_sync_mode.read()
    }

    /// The headers to send with a request now.
    pub(crate) fn request_headers(&self) -> crate::hyper_client::RequestHeaders {
        self.headers.request_headers()
    }

    /// The HTTP client requests go through.
    pub(crate) fn hyper_client(&self) -> &HyperClient {
        &self.hyper_client
    }

    /// Sends a GET to `url`, waiting as long as the request timeout, and
    /// returns what it answers, or an empty string if it does not.
    pub async fn ping(&self, url: &Url) -> String {
        ping_url(&self.hyper_client, &self.headers, url, self.options.timeout).await
    }

    /// Verifies that Kinetica is running at the cluster in use, failing over
    /// to another where it cannot be reached.
    ///
    /// The head node in use is sent a GET, waiting as long as the request
    /// timeout, and must answer exactly "Kinetica is running!". Any other
    /// answer fails with "Server returned invalid response: <answer>",
    /// without failing over. Where the head node cannot be reached, or
    /// answers an error status with nothing to read, the connection fails
    /// over and the next cluster is tried. Once every cluster has been
    /// tried, the failure is a connection error with the last attempt's
    /// message.
    pub async fn ping_with_failover(&self) -> Result<(), KineticaError> {
        let mut url = self.url().ok_or_else(|| KineticaError::Connection("No URL available".to_string()))?;
        let original_url = url.clone();
        let limit = (self.options.timeout > 0).then(|| std::time::Duration::from_millis(self.options.timeout));

        loop {
            debug!("Pinging URL: {url}");
            let failure = match self.hyper_client.ping_answer(url.as_str(), &self.headers.request_headers(), limit).await {
                Ok((status, reason, body)) if status >= 400 && body.is_empty() => {
                    KineticaError::Connection(format!("Server returned HTTP {status} ({reason})."))
                }
                Ok((_, _, body)) if body == "Kinetica is running!" => return Ok(()),
                Ok((_, _, body)) => return Err(KineticaError::Api(format!("Server returned invalid response: {body}"))),
                Err(e) => e,
            };
            url = match self.switch_url(&original_url, self.num_cluster_switches()).await {
                Ok(next) => next,
                // Every cluster has been tried, and circled back. Failover
                // switched off fails as such: only the HA failure is replaced.
                Err(KineticaError::HaUnavailable(_)) => return Err(KineticaError::Connection(failure.message())),
                Err(other) => return Err(other),
            };
        }
    }

    /// Sends a GET to `url`, waiting up to `timeout_ms` milliseconds (0 for
    /// no limit), and returns what it answers, or an empty string if it does
    /// not.
    pub async fn ping_with_timeout(&self, url: &Url, timeout_ms: u64) -> String {
        ping_url(&self.hyper_client, &self.headers, url, timeout_ms).await
    }

    /// Builds the client and processes its URLs, without the connectivity
    /// check that [`Kinetica::new`] then runs.
    pub(crate) async fn connect(urls: Vec<String>, options: KineticaOptions) -> Result<Self, KineticaError> {
        debug!("Initializing Kinetica client with {} URL(s)", urls.len());
        debug!("URLs: {urls:?}");

        if urls.is_empty() {
            let message = "Must provide at least one URL; gave none!";
            error!("{message}");
            return Err(KineticaError::Api(message.to_string()));
        }

        // The options' fields are public, so these values are checked when used.
        if options.max_total_connections < 1 {
            return Err(KineticaError::Config("max_total_connections must be greater than zero.".to_string()));
        }
        if options.max_connections_per_host < 1 {
            return Err(KineticaError::Config("max_connections_per_host must be greater than zero.".to_string()));
        }
        if options.initial_connection_attempt_timeout > MAX_INITIAL_CONNECTION_ATTEMPT_TIMEOUT_MS {
            return Err(KineticaError::Config(format!(
                "initial_connection_attempt_timeout must be at most 9223372036854 ms; given {}",
                options.initial_connection_attempt_timeout
            )));
        }

        // Parse URLs, each a comma-separated list
        let parsed_urls: Result<Vec<Url>, _> = urls.iter()
            .flat_map(|list| split_dropping_trailing_empty(list, ","))
            .map(|u| parse_absolute_url(u).map_err(KineticaError::Api))
            .collect();
        let parsed_urls = parsed_urls?;

        // Initialize hyper client. It speaks both http and https, choosing per
        // request from the URL scheme, and takes its timeouts and pool size
        // from the options.
        // A trust store named but unreadable fails the connection's creation
        let mut client_config = HttpClientConfig::from_options(&options);
        client_config.trust_store = crate::trust_store::from_options(&options)?;
        let hyper_client = HyperClient::with_config(&client_config);

        // The headers, set before any call is made: the caller's, whose Basic
        // credentials and HA synchronicity mode are taken, less the protected
        // ones, plus the User-Agent
        let mut http_headers = options.http_headers.clone();
        let (username, password) =
            credentials_from_http_headers(&http_headers, &options.username, &options.password)?;
        let authorization =
            crate::hyper_client::authorization_header(&options.oauth_token, &username, &password);
        let ha_sync_mode = match http_headers.get(HEADER_HA_SYNC_MODE) {
            Some(value) => HaSynchronicityMode::from_header(value)?,
            None => HaSynchronicityMode::Default,
        };
        for header in PROTECTED_HEADERS {
            http_headers.remove(header);
        }
        let client_name = options.client_name.clone();
        let client_version = options.client_version.clone();
        http_headers.insert(
            HEADER_USER_AGENT.to_string(),
            build_user_agent(client_name.as_deref(), client_version.as_deref()),
        );

        // Extract options into struct fields (before moving options)
        let use_snappy = options.use_snappy;
        let disable_failover = options.disable_failover;
        let disable_auto_discovery = options.disable_auto_discovery;
        let host_manager_port = options.host_manager_port;
        let initial_connection_attempt_timeout_ns = options.initial_connection_attempt_timeout * 1_000_000;
        let ha_failover_order = options.ha_failover_order;
        let primary_url = options.primary_url.clone();
        let hostname_regex = options.hostname_regex.as_deref()
            .map(HostnameRegex::compile)
            .transpose()?
            .flatten();

        // Convert URL to string and remove trailing slash to avoid double slashes in endpoint URLs
        let base_url = parsed_urls[0].to_string().trim_end_matches('/').to_string();

        // Each connection is a server session of its own, which scopes its SQL
        // #name temp tables to it, unless the caller names one to share
        let session = Session::new(options.session_id.as_deref());

        let mut kinetica = Self {
            url: base_url,
            options,  // Store the original KineticaOptions for public API access
            hyper_client,
            host_addresses: Arc::new(RwLock::new(Vec::new())),
            primary_url_hostname: RwLock::new(primary_url),
            ha_url_indices: Arc::new(RwLock::new(Vec::new())),
            current_cluster_index_pointer: Arc::new(RwLock::new(0)),
            num_cluster_switches: RwLock::new(0),
            username,
            password,
            headers: Arc::new(HeaderState {
                authorization,
                http_headers: RwLock::new(http_headers),
                ha_sync_mode: RwLock::new(ha_sync_mode),
                session_id: Some(session.id.clone()),
            }),
            client_name,
            client_version,
            hostname_regex,
            use_snappy,
            did_snappy_compression_size_warning: std::sync::atomic::AtomicBool::new(false),
            disable_failover,
            disable_auto_discovery: RwLock::new(disable_auto_discovery),
            host_manager_port,
            initial_connection_attempt_timeout_ns,
            ha_failover_order,
            failback_poller: parking_lot::Mutex::new(None),
            server_version: None,
            known_types: parking_lot::RwLock::new(HashMap::new()),
            switch_lock: Arc::new(tokio::sync::Mutex::new(())),
            probe_lock: tokio::sync::Mutex::new(()),
            session,
            session_cleanup_registered: false,
            closed: std::sync::atomic::AtomicBool::new(false),
        };

        // Process URLs and discover clusters
        kinetica.process_urls(parsed_urls).await?;

        // Drop the session even if this connection is never closed
        kinetica.session_cleanup_registered = true;

        debug!("Kinetica client initialized successfully, connected to {}", kinetica.url);

        // Log discovered cluster information
        let host_addresses = kinetica.host_addresses();
        if host_addresses.is_empty() {
            debug!("No cluster address info gathered (single-node or discovery disabled)");
        } else {
            debug!("Discovered {} cluster(s):", host_addresses.len());
            for (i, cluster_info) in host_addresses.iter().enumerate() {
                debug!("  Cluster {i}: {cluster_info}");
            }
        }

        Ok(kinetica)
    }

    // ==================== URL Processing Methods ====================

    /// Processes the given URLs, which may name the head node or worker ranks
    /// of any number of clusters, into the ring of clusters this connection
    /// uses.
    ///
    /// A failed attempt is retried for up to the initial connection attempt
    /// timeout, waiting a minute and then twice as long each time; with a
    /// timeout of 0 auto-discovery is disabled instead and the URLs used as
    /// given. When the hostname regex matches nothing the server advertises but
    /// does match some of the URLs given, those are used directly, head-node
    /// only. An unauthorized failure is never retried.
    async fn process_urls(&mut self, urls: Vec<Url>) -> Result<(), KineticaError> {
        // The very first time we sleep, if at all, will be for one minute
        let mut reattempt_wait_interval = std::time::Duration::from_secs(60);
        let start_time = std::time::Instant::now();

        let mut keep_trying = true;
        while keep_trying {
            debug!("Attempting to parse the user-given URLs: {}", url_list(&urls));
            match self.process_cluster_information_for_all_urls(urls.clone()).await {
                Ok(()) => {
                    debug!("Processed the cluster URLs successfully: {:?}", self.host_addresses());
                    return Ok(()); // one successful attempt is all we need
                }
                Err(KineticaError::HostnameRegexFailure(message)) => {
                    let accepted_urls =
                        resolution::urls_accepted_by_hostname_regex(&urls, self.hostname_regex.as_ref());
                    let pattern = self.hostname_regex.as_ref().map(|r| r.pattern().to_string()).unwrap_or_default();

                    if accepted_urls.is_empty() {
                        return Err(KineticaError::Api(format!(
                            "Could not connect to any working Kinetica server due to hostname regex mismatch \
                             (given URLs: {}): the pattern <{pattern}> matches no address the server advertises \
                             and none of the given URLs either; {message}",
                            url_list(&urls)
                        )));
                    }

                    // The regex eliminates every address the server advertises
                    // but matches a URL given: fall back to degraded mode.
                    warn!(
                        "Disabling auto-discovery & multi-head operations for this connection: the hostname \
                         regex <{pattern}> matches no address the server advertises, but does match {}, which \
                         will be used directly; {message}",
                        url_list(&accepted_urls)
                    );
                    *self.disable_auto_discovery.get_mut() = true;
                    return self.process_cluster_information_for_all_urls(accepted_urls).await;
                }
                Err(e) => {
                    if let KineticaError::Unauthorized(_) = e {
                        error!("Got Unauthorized while communicating to server, cannot proceed ..");
                        return Err(e);
                    }

                    debug!("Attempt at parsing URLs failed: {e}");

                    // If the user does not want us to retry, use the URLs as is
                    if self.initial_connection_attempt_timeout_ns == 0 {
                        // Report the demotion; multi-head is being given up
                        // because discovery failed and no retry was budgeted.
                        warn!(
                            "Disabling auto-discovery & multi-head operations for this connection \
                             (no retry: the initial connection attempt timeout is 0): {e}"
                        );
                        *self.disable_auto_discovery.get_mut() = true;
                    } else {
                        // The check comes before the sleep, and the sleep is not capped
                        // at the time left, so the first wait alone is a minute and the
                        // last can run past the timeout.
                        keep_trying = start_time.elapsed().as_nanos() as u64 <= self.initial_connection_attempt_timeout_ns;
                        debug!("Keep trying to parse URLs?: {keep_trying}");
                        if keep_trying {
                            let minutes = reattempt_wait_interval.as_secs() / 60;
                            warn!(
                                "Attempt at parsing user given URLs {} failed; waiting for {minutes} minute(s) before retrying",
                                url_list(&urls)
                            );
                            debug!("Sleeping for {minutes} minutes before trying again");
                            tokio::time::sleep(reattempt_wait_interval).await;
                            // The next time, we will sleep for twice as long
                            reattempt_wait_interval *= 2;
                        }
                    }
                }
            }
        }

        // Out of time: carry on with whatever clusters the last attempt
        // registered, and fail only if there are none.
        if self.ha_ring_size() == 0 {
            debug!("No cluster found!");
            return Err(KineticaError::Api(format!(
                "Could not connect to any working Kinetica server! Given URLs: {}",
                url_list(&urls)
            )));
        }
        Ok(())
    }

    /// Processes the given URLs, which may name the head node or worker ranks
    /// of any number of clusters, into this connection's ring of clusters.
    ///
    /// Each cluster is registered as it is found, so an attempt that fails part
    /// way leaves the clusters found before the failure. Clusters that are
    /// down, or whose properties cannot be read, are registered with just
    /// their URL. A user-given URL's cluster is reached at the head rank
    /// address the cluster advertises, searched across every candidate for it,
    /// or, if none answers, at the given URL, with no multi-head. The HA ring
    /// head nodes each cluster advertises are added to the URLs to process.
    async fn process_cluster_information_for_all_urls(&mut self, urls: Vec<Url>) -> Result<(), KineticaError> {
        self.host_addresses.write().clear();

        // De-duplicate the URLs, keeping their order, into a queue that the HA
        // ring addresses found along the way are added to
        let mut url_queue: VecDeque<Url> = VecDeque::new();
        for url in urls.iter() {
            if !url_queue.contains(url) {
                url_queue.push_back(url.clone());
            }
        }

        // If a fully qualified URL is given for the primary URL, process that
        // too; its host name is kept either way (it may be just a host name)
        let primary_url_str = self.options.primary_url.clone();
        *self.primary_url_hostname.get_mut() = primary_url_str.clone();
        if !primary_url_str.is_empty() {
            // Not a URL (just a host name, say): the text is the host name
            if let Ok(primary_url) = parse_absolute_url(&primary_url_str) {
                if !url_queue.contains(&primary_url) {
                    debug!("Primary URL not in user-given URLs; adding it");
                    url_queue.push_back(primary_url.clone());
                }
                *self.primary_url_hostname.get_mut() = primary_url.host_str().unwrap_or("").to_string();
            }
        }

        debug!(
            "Consolidated list of {} URLs to process: {}",
            url_queue.len(),
            url_list(url_queue.make_contiguous())
        );

        // API-discovered URLs are stored even if no server can be reached at
        // them (it might be temporarily down)
        let num_user_given_urls = url_queue.len();
        let mut num_processed_urls = 0;
        let mut is_user_given_url = true;

        // Whether all the user-given URLs belong to the same cluster, for the
        // purpose of choosing a primary
        let mut cluster_indices_of_user_given_urls: Vec<usize> = Vec::new();

        while let Some(url) = url_queue.pop_front() {
            if num_processed_urls >= num_user_given_urls {
                is_user_given_url = false;
            }
            num_processed_urls += 1;

            debug!(
                "Processing {} URL: {url}; remaining {} URL(s): {}",
                if is_user_given_url { "user-given" } else { "server-known" },
                url_queue.len(),
                url_list(url_queue.make_contiguous())
            );

            // Skip this URL if its host is in any already registered cluster
            let known_index = Self::index_of_cluster_containing_node(
                &self.host_addresses.read(),
                url.host_str().unwrap_or(""),
            );
            if let Some(index) = known_index {
                if is_user_given_url {
                    debug!("Skipping URL {url} (already found); adding index {index} to user-given processed cluster list");
                    cluster_indices_of_user_given_urls.push(index);
                } else {
                    debug!("Skipping URL {url} (already found)");
                }
                continue;
            }

            // Skip auto-discovery of cluster information if the user says so
            if *self.disable_auto_discovery.get_mut() {
                let index = self.host_addresses.write().len();
                if is_user_given_url {
                    debug!(
                        "Skipping connect verification of user-given URL {url} (auto-discovery disabled); \
                         adding index {index} to user-given processed cluster list"
                    );
                    cluster_indices_of_user_given_urls.push(index);
                } else {
                    debug!("Skipping connect verification of server-known URL {url} (auto-discovery disabled)");
                }
                let cluster_info = ClusterAddressInfo::with_host_manager_port(url, self.host_manager_port)?;
                debug!("Added cluster: {cluster_info}");
                self.host_addresses.write().push(cluster_info);
                continue;
            }

            // A cluster that is not running is registered anyway, with its HA
            // status; whether it could come up later is not known.
            let (is_running, ha_status) = self.get_system_running_status(&url, false).await?;
            if !is_running {
                let mut cluster_info = ClusterAddressInfo::with_host_manager_port(url.clone(), self.host_manager_port)?;
                cluster_info.set_ha_status(ha_status.clone());
                self.host_addresses.write().push(cluster_info);
                debug!(
                    "Added non-running cluster with {} URL: {url} (ha_status={ha_status:?})",
                    if is_user_given_url { "user-given" } else { "server-known" }
                );
                continue;
            }

            // Get the cluster's system properties; if that fails, skip to the
            // next URL, registering a server-known one anyway
            let system_properties = match self.get_system_properties_at_url(&url).await {
                Ok(properties) => properties,
                Err(_) => {
                    if is_user_given_url {
                        debug!("Skipping failed system properties lookup user-given URL: {url}");
                    } else {
                        let cluster_info = ClusterAddressInfo::with_host_manager_port(url, self.host_manager_port)?;
                        debug!("Added failed system properties lookup cluster with server-known URL: {cluster_info}");
                        self.host_addresses.write().push(cluster_info);
                    }
                    continue;
                }
            };

            // This could fail on a hostname regex mismatch
            let mut cluster_info = self.create_cluster_address_info(&url, system_properties.clone()).await?;
            cluster_info.set_ha_status(ha_status.clone());

            // For a user-given URL, verify connectivity to its cluster at that
            // cluster's own head rank address
            if is_user_given_url {
                let cluster_head_node_url = cluster_info.active_head_node_url().clone();

                if url != cluster_head_node_url {
                    // Try every server-advertised address for the head rank; if
                    // none answers, use the given URL head-node-only.
                    let server_is_local = resolution::is_local_address(url.host_str().unwrap_or("")).await;
                    let Some(reachable_head_node_url) = self
                        .resolve_reachable_head_node_url(&system_properties, &cluster_head_node_url, server_is_local)
                        .await
                    else {
                        // Recorded, not announced, until a connection is
                        // attempted to this cluster.
                        debug!(
                            "Multi-head operations unavailable for the cluster reached via <{url}>: reachable with \
                             the user-given URL but not with its server-known URL <{cluster_head_node_url}>"
                        );

                        // Record the lost capability against THIS cluster only,
                        // keyed on the URL that does work, and carry on.
                        let mut degraded = ClusterAddressInfo::with_host_manager_port(url, self.host_manager_port)?;
                        degraded.set_ha_status(ha_status);
                        degraded.set_multi_head_available(false);

                        cluster_indices_of_user_given_urls.push(self.host_addresses.write().len());
                        self.host_addresses.write().push(degraded);
                        continue;
                    };

                    // A head rank answered, so the cluster's advertised
                    // address space is reachable and multi-head is viable.
                    // That is all the probe establishes: it does not establish
                    // that what answered is this cluster. A server advertising
                    // an address that resolves elsewhere from here (loopback, a
                    // container bridge present on every host, a name that
                    // exists in two namespaces) sends the probe to a different
                    // Kinetica, which answers exactly as the right one would.
                    //
                    // So the connection stays on the address the caller gave.
                    // It is the only one with an authority behind it: the
                    // caller asserted it, and the cluster's own properties
                    // arrived through it. Every other candidate is the server's
                    // self-report, which is the thing in question.
                    if reachable_head_node_url != cluster_head_node_url {
                        debug!(
                            "Head rank did not answer at its preferred address <{cluster_head_node_url}> but did \
                             at <{reachable_head_node_url}>; multi-head stays enabled and the connection continues \
                             through the user-given URL <{url}>"
                        );
                    }

                    let host_manager_url = self.build_host_manager_url(
                        &url,
                        resolution::is_httpd_enabled(&system_properties),
                        &system_properties,
                    )?;
                    cluster_info.set_active_head_node_url(url.clone());
                    cluster_info.set_host_manager_url(host_manager_url);

                    debug!(
                        "Verified connectivity with user-given URL {url} at server-known URL {}; adding index {} \
                         to user-given processed cluster list",
                        cluster_info.active_head_node_url(),
                        self.host_addresses.write().len()
                    );
                }

                cluster_indices_of_user_given_urls.push(self.host_addresses.write().len());
            }

            debug!("Added URL {url} -> cluster {cluster_info}");
            self.host_addresses.write().push(cluster_info);

            // Add the HA ring head nodes in the properties to the queue, if not
            // processed already; this could fail on a hostname regex mismatch
            let ha_ring_head_node_urls =
                resolution::ha_ring_head_node_urls(&system_properties, self.hostname_regex.as_ref())?;
            debug!("Got HA ring head node URLs: {}", url_list(&ha_ring_head_node_urls));
            for ha_url in ha_ring_head_node_urls {
                let known = Self::index_of_cluster_containing_node(
                    &self.host_addresses.read(),
                    ha_url.host_str().unwrap_or(""),
                );
                if known.is_none() {
                    if !url_queue.contains(&ha_url) {
                        debug!("HA ring head node URL {ha_url} not found in known clusters; adding to queue to process");
                        url_queue.push_back(ha_url);
                    }
                } else {
                    debug!("HA ring head node URL {ha_url} found in known clusters; skipping");
                }
            }
        }

        // Check that we have got at least one working URL
        if self.ha_ring_size() == 0 {
            error!("No clusters found at user given URLs {}!", url_list(&urls));
            return Err(KineticaError::Api(format!(
                "Could not connect to any working Kinetica server, given URLs: {}",
                url_list(&urls)
            )));
        }

        // Set the primary cluster and head node
        if self.ha_ring_size() == 1 {
            let mut host_addresses = self.host_addresses.write();
            host_addresses[0].set_primary_cluster(true);
            let new_primary_url = host_addresses[0].active_head_node_url().clone();
            *self.primary_url_hostname.get_mut() = new_primary_url.host_str().unwrap_or("").to_string();
            debug!(
                "Updated primary URL {} -> {new_primary_url} for single-cluster connection",
                self.options.primary_url
            );
            self.options.primary_url = new_primary_url.to_string();
        } else if self.primary_url_hostname.get_mut().is_empty() {
            // With no primary given, if all the user-given URLs belong to a
            // single cluster, that is the primary
            let distinct: HashSet<usize> = cluster_indices_of_user_given_urls.iter().copied().collect();
            if distinct.len() == 1 {
                let primary_index = cluster_indices_of_user_given_urls[0];
                let new_primary_url = self.host_addresses.write()[primary_index].active_head_node_url().clone();
                *self.primary_url_hostname.get_mut() = new_primary_url.host_str().unwrap_or("").to_string();
                debug!(
                    "Updated primary host name {} -> {new_primary_url} for multi-cluster connection",
                    self.options.primary_url
                );
                self.options.primary_url = new_primary_url.to_string();
            } else {
                debug!(
                    "Could not update primary host name for multi-cluster connection, as user-given URLs belong to \
                     different clusters"
                );
            }
        }

        // Flag the primary cluster as such and ensure it is the first one
        let primary_hostname = self.primary_url_hostname.get_mut().clone();
        if !primary_hostname.is_empty() {
            let primary_index = Self::index_of_cluster_containing_node(&self.host_addresses.read(), &primary_hostname);
            debug!("Checking if the primary cluster is in the ring; index: {primary_index:?}");
            match primary_index {
                Some(primary_index) => {
                    debug!("Setting that cluster as primary");
                    let mut host_addresses = self.host_addresses.write();
                    host_addresses[primary_index].set_primary_cluster(true);
                    if primary_index > 0 {
                        debug!("Moving primary cluster to the front of the list");
                        let primary = host_addresses.remove(primary_index);
                        host_addresses.insert(0, primary);
                    }
                }
                None => debug!("Designated primary cluster with host {primary_hostname} not found in cluster list."),
            }
        }

        // Order the clusters for failover, the primary first
        self.randomize_urls();

        // Announce a degraded connection exactly once, and only for the cluster
        // we are actually going to talk to; a failover onto one is announced
        // when it happens. The reprocessing after auto-discovery is disabled
        // builds every cluster bare, which this check stays silent on, so it
        // does not double up with what process_urls() reports.
        if let Some(active_cluster) = self.cluster_info() {
            if !active_cluster.is_probe_owed() && !active_cluster.is_multi_head_available() {
                warn!(
                    "Multi-head operations unavailable for the cluster being connected to <{}>: \
                     its worker addresses are not reachable from this client. \
                     Operations will use the head node only.",
                    active_cluster.active_head_node_url()
                );
            }
        }

        Ok(())
    }

    /// Finds the index of the cluster containing the given hostname.
    fn index_of_cluster_containing_node(host_addresses: &[ClusterAddressInfo], hostname: &str) -> Option<usize> {
        host_addresses.iter().position(|info| info.does_cluster_contain_node(hostname))
    }

    // ==================== System Status and Properties Methods ====================

    /// Whether the system at `url` reports itself running, by a quick check
    /// through the fast client (a 2-second connect and response, no retries),
    /// so that probing a rank that is down or hung costs seconds whatever the
    /// connection's timeout.
    ///
    /// Fails only with [`KineticaError::Unauthorized`]; any other failure
    /// is logged and reported as not running.
    async fn is_system_running(&self, url: &Url) -> Result<bool, KineticaError> {
        Ok(self.get_system_running_status(url, true).await?.0)
    }

    /// Whether the system at `url` is running, and the `drained` value of its
    /// HA status if it reports one.
    ///
    /// Running means the `system` entry of `/show/system/status` is JSON whose
    /// `status` is exactly `running`. A missing entry, an answer that cannot be
    /// read, or any failure other than an authentication one means "not
    /// running", with a warning logged. A `quick_check` goes through the fast
    /// client.
    async fn get_system_running_status(
        &self,
        url: &Url,
        quick_check: bool,
    ) -> Result<(bool, Option<String>), KineticaError> {
        let null = |v: &Option<String>| v.clone().unwrap_or_else(|| "null".to_string());
        let checked = async {
            let status_map = self.get_system_status_at_url(url, quick_check).await?;

            let Some(system_status) = status_map.get("system") else {
                warn!("No system status entry for URL {url}");
                return Ok((false, None));
            };
            let system_status_info: serde_json::Value = serde_json::from_str(system_status)
                .map_err(|e| KineticaError::Api(e.to_string()))?;
            let ha_status_info: Option<serde_json::Value> = status_map
                .get("ha_status")
                .map(|s| serde_json::from_str(s))
                .transpose()
                .map_err(|e| KineticaError::Api(e.to_string()))?;

            let is_running = system_status_info.get("status").and_then(|v| v.as_str()) == Some("running");

            // A system with a draining HA queue is still running; the drain
            // state is reported separately, for is_cluster_usable to consult.
            let ha_status = ha_status_info
                .as_ref()
                .and_then(|info| info.get("drained"))
                .and_then(|drained| drained.as_str())
                .map(str::to_string);

            if is_running {
                debug!("System running at URL {url} (ha_status={})", null(&ha_status));
            } else {
                warn!(
                    "System not confirmed running at URL {url} (running={is_running}, ha_status={})",
                    null(&ha_status)
                );
            }
            Ok((is_running, ha_status))
        }
        .await;

        match checked {
            Ok(result) => Ok(result),
            Err(KineticaError::Unauthorized(msg)) => Err(KineticaError::Unauthorized(msg)),
            Err(e) => {
                // Any error means we don't know whether the system is running
                warn!("Error checking running status of URL {url} -- {e}");
                Ok((false, None))
            }
        }
    }

    /// Whether the cluster at `url` can serve as the active cluster: running
    /// and not draining its HA queue: the acceptance test failover applies to
    /// each candidate cluster. Never fails; an error counts as "not usable".
    async fn is_cluster_usable(&self, url: &Url, quick_check: bool) -> bool {
        match self.get_system_running_status(url, quick_check).await {
            Ok((running, drained)) => running && drained.as_deref() != Some("draining"),
            Err(e) => {
                warn!("Cluster at {url} deemed not usable: {e}");
                false
            }
        }
    }

    /// Gets the `status_map` of `/show/system/status` at the given URL, through
    /// the fast client for a quick check.
    async fn get_system_status_at_url(&self, url: &Url, quick_check: bool) -> Result<HashMap<String, String>, KineticaError> {
        use crate::{ShowSystemStatusRequest, ShowSystemStatusResponse};
        use std::sync::OnceLock;

        static REQ_SCHEMA: OnceLock<FastSchema> = OnceLock::new();
        static RESP_SCHEMA: OnceLock<FastSchema> = OnceLock::new();

        let req_schema = REQ_SCHEMA.get_or_init(|| {
            FastSchema::from_str(r#"{"fields":[{"name":"options","type":{"type":"map","values":"string"}}],"name":"show_system_status_request","type":"record"}"#)
                .expect("Failed to parse show_system_status request schema")
        });
        let resp_schema = RESP_SCHEMA.get_or_init(|| {
            FastSchema::from_str(r#"{"fields":[{"name":"status_map","type":{"type":"map","values":"string"}},{"name":"info","type":{"type":"map","values":"string"}}],"name":"show_system_status_response","type":"record"}"#)
                .expect("Failed to parse show_system_status response schema")
        });

        debug!("Getting system status for URL: {url}");
        let endpoint = append_path_to_url(url, "/show/system/status").to_string();
        let request = ShowSystemStatusRequest::default();
        let response: ShowSystemStatusResponse =
            self.submit_request_to_url(&endpoint, req_schema, resp_schema, &request, quick_check).await?;

        trace!("System status response from {}: {:?}", url, response.status_map);
        Ok(response.status_map)
    }

    /// Gets system properties from the given URL.
    ///
    /// This method retrieves cluster configuration information needed for
    /// auto-discovery of workers and HA ring members.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to query
    ///
    /// # Returns
    ///
    /// Returns a map of system properties or an error.
    async fn get_system_properties_at_url(&self, url: &Url) -> Result<HashMap<String, String>, KineticaError> {
        use crate::{ShowSystemPropertiesRequest, ShowSystemPropertiesResponse};
        use std::sync::OnceLock;

        static REQ_SCHEMA: OnceLock<FastSchema> = OnceLock::new();
        static RESP_SCHEMA: OnceLock<FastSchema> = OnceLock::new();

        let req_schema = REQ_SCHEMA.get_or_init(|| {
            FastSchema::from_str(r#"{"fields":[{"name":"options","type":{"type":"map","values":"string"}}],"name":"show_system_properties_request","type":"record"}"#)
                .expect("Failed to parse show_system_properties request schema")
        });
        let resp_schema = RESP_SCHEMA.get_or_init(|| {
            FastSchema::from_str(r#"{"fields":[{"name":"property_map","type":{"type":"map","values":"string"}},{"name":"info","type":{"type":"map","values":"string"}}],"name":"show_system_properties_response","type":"record"}"#)
                .expect("Failed to parse show_system_properties response schema")
        });

        let endpoint = append_path_to_url(url, "/show/system/properties").to_string();
        let request = ShowSystemPropertiesRequest::default();
        let response: ShowSystemPropertiesResponse =
            self.submit_request_to_url(&endpoint, req_schema, resp_schema, &request, false).await?;

        Ok(response.property_map)
    }

    /// Submits a request to a specific URL (used during initialization).
    ///
    /// This is similar to `submit_request` but takes a URL string
    /// directly instead of using the configured base URL. A `fast` request
    /// goes through the fast client; see [`HyperClient::post_binary_raw_fast`].
    async fn submit_request_to_url<Req, Resp>(
        &self,
        url: &str,
        req_schema: &FastSchema,
        resp_schema: &FastSchema,
        request: &Req,
        fast: bool,
    ) -> Result<Resp, KineticaError>
    where
        Req: KineticaRecord + Serialize,
        Resp: DeserializeOwned
    {
        // Encode request to Avro using serde_avro_fast
        let mut config = serde_avro_fast::ser::SerializerConfig::new(req_schema);
        let req_bytes = serde_avro_fast::to_datum(request, Vec::new(), &mut config)
            .map_err(|e| KineticaError::Avro(format!("Request encode failed: {e}")))?;

        // Send request, uncompressed
        let resp = if fast {
            self.hyper_client.post_binary_raw_fast(url, req_bytes, &self.request_headers()).await?
        } else {
            self.hyper_client.post_binary_raw(url, req_bytes, &self.request_headers(), false).await?
        };

        // Interpreted as any other response: only a 401 or 403 is an
        // authorization failure, whatever a server error's message says.
        let wrapper = interpret_response(url, &resp)?;

        // Decode the payload. A quick check, which goes through the fast
        // client, is not checked for the session being used.
        let decoded = serde_avro_fast::from_datum_slice::<Resp>(&wrapper.data, resp_schema)
            .map_err(|e| KineticaError::Http(format!("Payload decode failed: {e}")))?;
        if !fast {
            self.note_session_used(&wrapper.data, resp_schema);
        }
        Ok(decoded)
    }

    /// The host manager address for the host a connection is addressed by.
    ///
    /// Prefers what the deployment publishes,
    /// `conf.host<i>_host_manager_public_url` for the host `head_node_url`
    /// names. The two derived forms are not interchangeable: under httpd every
    /// host proxies the host-manager path to the *head* node's host manager,
    /// while the direct port reaches whichever host was addressed, and some
    /// host-manager endpoints are head-only. Deriving therefore picks a host
    /// manager rather than an address form, which the operator's published
    /// address settles.
    ///
    /// Failing that, behind httpd the head node's protocol, host and **port**
    /// are kept and only the path's last segment is replaced (see
    /// [`resolution::host_manager_path_for`]). Httpd takes two signals: the
    /// server reporting it, and this URL carrying a path — a caller on a
    /// cluster with httpd may still be connected straight to the database
    /// port. Otherwise the host manager port is used with no path: the
    /// server's `conf.hm_http_port` where the caller left the port at its
    /// default, since deployments are found on other ports, and the caller's
    /// port where they chose one.
    fn build_host_manager_url(
        &self,
        head_node_url: &Url,
        is_httpd_enabled: bool,
        system_properties: &HashMap<String, String>,
    ) -> Result<Url, KineticaError> {
        if let Some(published) = resolution::published_host_manager_url(system_properties, head_node_url) {
            debug!("Using the published host manager URL: {published}");
            return Ok(published);
        }

        let mut host_manager_url = head_node_url.clone();
        // A lone `/` is no path
        if is_httpd_enabled && head_node_url.path() != "/" {
            host_manager_url.set_path(&resolution::host_manager_path_for(head_node_url));
        } else {
            let port = if self.host_manager_port == DEFAULT_HOST_MANAGER_PORT {
                resolution::published_host_manager_port(system_properties, i32::from(self.host_manager_port))
            } else {
                i32::from(self.host_manager_port)
            };
            // -1 is the scheme's default port, and anything below it is
            // refused. A port above 65535, which a URL here cannot hold, is
            // refused too.
            let port = match port {
                -1 => None,
                0..=65535 => Some(port as u16),
                _ => {
                    return Err(KineticaError::Api(format!(
                        "Error creating the host manager URL: port {port} is out of range"
                    )));
                }
            };
            host_manager_url
                .set_port(port)
                .map_err(|_| KineticaError::Api("Error creating the host manager URL".to_string()))?;
            host_manager_url.set_path("");
        }
        host_manager_url.set_query(None);
        host_manager_url.set_fragment(None);
        debug!("Created host manager URL: {host_manager_url}");
        Ok(host_manager_url)
    }

    /// Creates a ClusterAddressInfo from a URL and the cluster's system
    /// properties: the head rank URL, the worker rank URLs, the host names
    /// and the host manager URL.
    ///
    /// Where the server `url` reaches is on another machine, an advertised
    /// rank address naming this machine withdraws the whole rank list (see
    /// [`resolution::rank_urls_if_all_credible`]), and the head becomes `url`.
    async fn create_cluster_address_info(
        &self,
        url: &Url,
        system_properties: HashMap<String, String>,
    ) -> Result<ClusterAddressInfo, KineticaError> {
        debug!("Establishing a cluster record associated with URL: {url}");

        let is_httpd_enabled = resolution::is_httpd_enabled(&system_properties);

        // Whether the server the caller reached is on this machine. Decided
        // from the caller's address, not from the advertised ones: addressing
        // one's own machine by a routable address is an ordinary choice, so
        // the caller writing a non-loopback URL says nothing on its own.
        let server_is_local = resolution::is_local_address(url.host_str().unwrap_or("")).await;

        // The rank URLs, head first. All or nothing: the list is rank-indexed
        // and an empty slot already means "rank removed", so one entry cannot
        // be marked unusable without shifting the rest. The rank-0 slot is
        // removed whatever it holds, so that the remaining worker URLs stay
        // aligned with the rank numbering.
        let mut rank_urls = resolution::rank_urls_if_all_credible(
            resolution::extract_rank_urls(&system_properties, self.hostname_regex.as_ref())?,
            server_is_local,
            "rank address",
        )
        .await;
        let active_head_node_url = if !rank_urls.is_empty() {
            debug!("Assigning head rank URL {:?} from server-known rank URLs: {rank_urls:?}", rank_urls[0]);
            // Rank 0 cannot be removed from a cluster, but do not hand out a
            // missing head URL should the server ever report a blank slot.
            rank_urls.remove(0).unwrap_or_else(|| url.clone())
        } else {
            debug!("Assigning head rank URL to the user-given one {url}, as no server-known worker rank URLs found");
            url.clone()
        };

        let host_names =
            resolution::host_names_from_system_properties(&system_properties, self.hostname_regex.as_ref())?;

        let host_manager_url = self.build_host_manager_url(&active_head_node_url, is_httpd_enabled, &system_properties)?;

        let mut cluster_info = ClusterAddressInfo::new(
            active_head_node_url,
            system_properties,
            rank_urls,
            host_names,
            host_manager_url,
        );

        // Check if this is the primary cluster
        let primary_hostname = self.primary_url_hostname.read();
        if !primary_hostname.is_empty() && cluster_info.does_cluster_contain_node(&primary_hostname) {
            cluster_info.set_primary_cluster(true);
            debug!("Marked this cluster as primary");
        }

        Ok(cluster_info)
    }

    /// Builds the order in which the HA clusters are tried: sequential, or for
    /// a random order shuffled, keeping the primary cluster first when one was
    /// given.
    fn randomize_urls(&self) {
        let host_addresses = self.host_addresses.read();
        let mut indices: Vec<usize> = (0..host_addresses.len()).collect();

        if self.ha_failover_order == HaFailoverOrder::Random {
            let primary_hostname = self.primary_url_hostname.read().clone();
            let mut rng = rand::rng();
            if primary_hostname.is_empty() {
                debug!("Randomizing all clusters for HA failover--no primary host given");
                indices.shuffle(&mut rng);
            } else {
                debug!(
                    "Randomizing all cluster for HA failover except for primary host {primary_hostname}"
                );
                if indices.len() > 2 {
                    indices[1..].shuffle(&mut rng);
                }
            }
        }

        *self.ha_url_indices.write() = indices;
    }

    // ==================== Getter Methods ====================

    /// Gets the list of URLs of the active head ranks of all clusters.
    pub fn urls(&self) -> Vec<Url> {
        self.host_addresses.read()
            .iter()
            .map(|info| info.active_head_node_url().clone())
            .collect()
    }

    /// Gets the list of URLs in failover order.
    pub fn failover_urls(&self) -> Vec<Url> {
        let host_addresses = self.host_addresses.read();
        let indices = self.ha_url_indices.read();
        indices.iter()
            .filter_map(|&i| host_addresses.get(i))
            .map(|info| info.active_head_node_url().clone())
            .collect()
    }

    /// Gets the list of host manager URLs.
    pub fn hm_urls(&self) -> Vec<Url> {
        self.host_addresses.read()
            .iter()
            .map(|info| info.host_manager_url().clone())
            .collect()
    }

    /// Gets the URL of the host manager of the cluster in use.
    pub fn hm_url(&self) -> Option<Url> {
        self.cluster_info().map(|info| info.host_manager_url().clone())
    }

    /// Gets the active cluster's information.
    pub fn cluster_info(&self) -> Option<ClusterAddressInfo> {
        let slot = self.current_cluster_slot()?;
        self.host_addresses.read().get(slot).cloned()
    }

    /// Resolves the index into `host_addresses` of the cluster currently being
    /// talked to.
    ///
    /// [`Kinetica::cluster_info`] hands back a clone, which is all most
    /// callers need. A caller that has to *record* something against the
    /// current cluster — see
    /// [`Kinetica::probe_current_cluster_multi_head`] — needs the slot instead,
    /// since writing to a clone would discard the observation.
    fn current_cluster_slot(&self) -> Option<usize> {
        let host_addresses = self.host_addresses.read();
        let current_index = *self.current_cluster_index_pointer.read();
        cluster_slot(&host_addresses, &self.ha_url_indices.read(), current_index)
    }

    /// Gets the current active URL.
    pub fn url(&self) -> Option<Url> {
        self.cluster_info().map(|info| info.active_head_node_url().clone())
    }

    /// Gets a copy of the list of ClusterAddressInfo objects (HA ring info).
    pub fn ha_ring_info(&self) -> Vec<ClusterAddressInfo> {
        self.host_addresses.read().clone()
    }

    /// Gets the number of clusters in the HA ring.
    pub fn ha_ring_size(&self) -> usize {
        self.host_addresses.read().len()
    }

    /// Gets the list of all host addresses.
    pub fn host_addresses(&self) -> Vec<ClusterAddressInfo> {
        self.host_addresses.read().clone()
    }

    /// Checks whether auto-discovery is enabled.
    ///
    /// Auto-discovery allows the client to automatically discover worker nodes
    /// and HA ring members from the server. When disabled, the client uses
    /// only the explicitly provided URLs.
    ///
    /// # Returns
    ///
    /// `true` if auto-discovery is enabled, `false` otherwise.
    pub fn is_auto_discovery_enabled(&self) -> bool {
        !*self.disable_auto_discovery.read()
    }

    /// Re-reads the current cluster's worker rank addresses from that cluster's
    /// own system properties and records whether multi-head operations are
    /// usable there.
    ///
    /// This is a **capability probe**, not a health check: a cluster whose head
    /// node is up is usable whatever its workers are doing, and the answer here
    /// only decides multi-head versus head-node-only. Callers in the failover
    /// path must not treat `false` as a reason to reject the cluster — doing so
    /// kills a failover to a cluster that would have accepted the work through
    /// its head node.
    ///
    /// The refresh matters because the cluster list is populated once during
    /// connection setup and never refreshed per failover; without it the probe
    /// would validate a construction-time snapshot of a topology that may have
    /// moved since.
    ///
    /// Resolves addresses with the *connection's* hostname regex, never a
    /// caller's. What this writes — the cluster's worker addresses and whether
    /// multi-head is available there — is connection-scoped state that every
    /// multi-head object reads, so letting one object's filter decide it would
    /// let that object's address selection determine a fact about the cluster
    /// for all of them.
    ///
    /// # Returns
    ///
    /// Whether multi-head operations are available on the current cluster.
    pub async fn probe_current_cluster_multi_head(&self) -> bool {
        // One probe at a time: the addresses and the verdict it publishes are
        // not a pair that can be interleaved with another probe's.
        let _probing = self.probe_lock.lock().await;
        self.probe_current_cluster_multi_head_locked().await
    }

    /// The probe itself, run under `probe_lock`.
    async fn probe_current_cluster_multi_head_locked(&self) -> bool {
        let Some(slot) = self.current_cluster_slot() else {
            return false;
        };

        // Take what the probe needs out from under the lock; everything below
        // awaits, and the lock must not be held across an await point.
        let (was_available, head_node_url, held_worker_urls) = {
            let host_addresses = self.host_addresses.read();
            let Some(cluster) = host_addresses.get(slot) else {
                return false;
            };
            (
                cluster.is_multi_head_available(),
                cluster.active_head_node_url().clone(),
                cluster.worker_rank_urls().to_vec(),
            )
        };

        // Resolution draws on four properties, ordered so that addresses an
        // operator configured are tried before ones the server discovered for
        // itself, and probed so that a rank is only written off once every
        // address advertised for it has failed.
        let probed = async {
            let properties = self.get_system_properties_at_url(&head_node_url).await?;
            let retained = resolution::retain_matching_rank_candidate_urls(
                &resolution::extract_rank_candidate_urls(&properties),
                self.hostname_regex.as_ref(),
            )?;
            let rank_host = resolution::extract_rank_host_mapping(&properties, retained.len());
            // A server on this machine is told apart from a second instance
            // on another port by its own report
            let reference = if resolution::is_local_address(head_node_url.host_str().unwrap_or("")).await {
                Some(resolution::cluster_identity_signature(&properties))
            } else {
                None
            };
            Ok::<_, KineticaError>(
                self.probe_rank_candidate_urls(&retained, &rank_host, &head_node_url, reference.as_deref()).await,
            )
        }
        .await;

        let (worker_urls, mut available, probe_decided) = match probed {
            Ok((available, selected)) => (selected, available, true),
            Err(e) => {
                debug!("Could not refresh worker addresses for cluster <{head_node_url}>: {e}");
                (held_worker_urls, false, false)
            }
        };

        if worker_urls.is_empty() {
            available = false;
        } else if !probe_decided {
            // Fell back to the addresses already held, which carry no
            // candidates to search: probe them as they are. Empty slots are
            // ranks removed from the cluster, not ranks that are down.
            available = true;
            for worker_rank in worker_urls.iter().flatten() {
                if !self.is_rank_running(worker_rank).await {
                    available = false;
                    break;
                }
            }
        }

        {
            // The single publication: a new snapshot carries the addresses and
            // the verdict together, and its identity tells every multi-head
            // object that this cluster's answer has been replaced. The arrival's
            // probe is then done; the marker is cleared with it, so a caller
            // that sees it clear sees the snapshot that goes with it.
            let mut host_addresses = self.host_addresses.write();
            if let Some(cluster) = host_addresses.get_mut(slot) {
                cluster.set_multi_head_snapshot(Arc::new(MultiHeadSnapshot::new(worker_urls, available)));
                cluster.set_probe_owed(false);
            }
        }

        // Announce only the transition. A cluster already known to be degraded
        // is reported when the switch onto it happens; warning again here would
        // double up on every failover onto it.
        if was_available && !available {
            warn!(
                "Multi-head operations are no longer available on cluster <{head_node_url}>: \
                 its worker addresses are not reachable from this client. \
                 Operations will use the head node only."
            );
        }

        available
    }

    /// Probes each rank's candidate URLs and records the one that answered.
    ///
    /// Ordered by **host** rather than by rank: the first candidate that
    /// answers for any rank on a host has its address noted as that host's
    /// preferred one, and the host's other ranks try their candidate on that
    /// address first. A success settles the preferred address for the host,
    /// not the other ranks, which are each still probed; a failure settles
    /// nothing; and the preference is an ordering, never a restriction. The
    /// head node's own address seeds its host's preference, unless it names
    /// more than one host.
    ///
    /// Returns whether every live rank answered on some candidate, and the
    /// worker slots (entry `i` is rank `i + 1`), with `None` for a removed rank
    /// and an unreachable rank's first candidate.
    ///
    /// `reference_signature` is the identity signature of the cluster being
    /// probed, or `None` to accept any address that answers; it is passed
    /// through to each rank's search.
    async fn probe_rank_candidate_urls(
        &self,
        candidate_urls: &resolution::RankCandidates,
        rank_host: &[Option<usize>],
        head_node_url: &Url,
        reference_signature: Option<&str>,
    ) -> (bool, Vec<Option<Url>>) {
        // Keyed by the full address text
        let mut answered_by_address: HashMap<String, bool> = HashMap::new();
        let mut preferred_host_by_host: HashMap<usize, String> = HashMap::new();
        let mut different_cluster_by_address: HashMap<String, bool> = HashMap::new();

        // An address naming more than one host cannot stand for any of them
        let non_identifying = resolution::non_identifying_hosts(candidate_urls, rank_host);

        if let (Some(Some(head_host)), Some(head_name)) = (rank_host.first(), head_node_url.host_str()) {
            if !non_identifying.contains(head_name) {
                preferred_host_by_host.insert(*head_host, head_name.to_string());
            }
        }

        let mut all_reachable = true;
        let mut selected = Vec::new();

        // Slot 0 is the head rank, which is not a worker and is not probed
        for (rank, candidates) in candidate_urls.iter().enumerate().skip(1) {
            let Some(candidates) = candidates else {
                selected.push(None); // slot kept for a removed rank
                continue;
            };

            let host = rank_host.get(rank).copied().flatten();
            let preferred_host = host.and_then(|h| preferred_host_by_host.get(&h).cloned());

            let answered = self
                .get_running_rank_url(
                    &resolution::order_urls_by_preferred_host_first(candidates, preferred_host.as_deref()),
                    Some(&mut answered_by_address),
                    &resolution::rank_label(rank),
                    reference_signature,
                    &mut different_cluster_by_address,
                )
                .await;

            let Some(answered) = answered else {
                // Keep the first so the published addresses still describe what
                // would be used, and report the addresses tried.
                let tried: Vec<&str> = candidates.iter().map(Url::as_str).collect();
                debug!("{} answered on none of {tried:?}", resolution::rank_label(rank));
                selected.push(candidates.first().cloned());
                all_reachable = false;
                continue;
            };

            // A success settles the address for the host, not the ranks, and
            // only where the address identifies that host in the first place.
            if let Some(host) = host {
                let answered_host = answered.host_str().unwrap_or("").to_string();
                if non_identifying.contains(&answered_host) {
                    debug!(
                        "{} answered at <{answered}>, which names more than one host; using it for this rank only",
                        resolution::rank_label(rank)
                    );
                } else {
                    preferred_host_by_host.entry(host).or_insert(answered_host);
                }
            }
            selected.push(Some(answered));
        }

        (all_reachable, selected)
    }

    /// The first of a rank's candidate URLs that answers, or `None`: the one
    /// search shared by the head rank check at connect and the per-rank search
    /// of a probe. `answered_by_address` memoizes outcomes across ranks, keyed
    /// by the full address text.
    ///
    /// Where `reference_signature` is given, a candidate that answers but
    /// reports a different cluster (see [`Self::is_different_cluster`]) is
    /// passed over as if it had not answered: answering is not belonging.
    /// `different_cluster_by_address` memoizes those verdicts, keyed by the
    /// full address text, and is unused without a reference.
    async fn get_running_rank_url(
        &self,
        candidates: &[Url],
        mut answered_by_address: Option<&mut HashMap<String, bool>>,
        what: &str,
        reference_signature: Option<&str>,
        different_cluster_by_address: &mut HashMap<String, bool>,
    ) -> Option<Url> {
        for candidate in candidates {
            let address = candidate.to_string();
            let known = answered_by_address.as_ref().and_then(|memo| memo.get(&address).copied());
            let answered = match known {
                Some(answered) => answered,
                None => {
                    let answered = self.is_rank_running(candidate).await;
                    if let Some(memo) = answered_by_address.as_mut() {
                        memo.insert(address, answered);
                    }
                    answered
                }
            };
            if answered {
                // The only check that reaches two clusters on one machine,
                // where no address is remote and the locality filter abstains
                if let Some(reference) = reference_signature {
                    if self.is_different_cluster(candidate, reference, different_cluster_by_address).await {
                        continue;
                    }
                }
                return Some(candidate.clone());
            }
            debug!("{what} did not answer at <{candidate}>");
        }
        None
    }

    /// Whether a candidate address demonstrably belongs to a different
    /// cluster.
    ///
    /// Rejects on evidence only. A differing signature (see
    /// [`resolution::cluster_identity_signature`]) proves the addresses are
    /// different clusters, while a matching one proves nothing and is no
    /// objection. Where the candidate's properties cannot be read it is
    /// accepted, because an unreachable address fails its own probe and one
    /// that will not answer this is not evidence of anything. An empty
    /// `reference` rejects nothing.
    ///
    /// The candidate has already answered a liveness probe; its properties are
    /// fetched here, a round trip the first time an address is judged. `seen`
    /// holds the verdicts by full address text rather than by host, since two
    /// clusters on one machine differ only by port.
    async fn is_different_cluster(&self, candidate: &Url, reference: &str, seen: &mut HashMap<String, bool>) -> bool {
        if reference.is_empty() {
            return false; // nothing to compare against
        }

        let address = candidate.to_string();
        if let Some(&decided) = seen.get(&address) {
            return decided;
        }

        let different = match self.get_system_properties_at_url(candidate).await {
            Ok(properties) => {
                let signature = resolution::cluster_identity_signature(&properties);
                let different = !signature.is_empty() && signature != reference;
                if different {
                    debug!(
                        "Address <{candidate}> answers, but reports a different cluster than the one reached; \
                         skipping it"
                    );
                }
                different
            }
            Err(e) => {
                debug!("Could not read properties at <{candidate}> to compare clusters: {e}");
                false
            }
        };

        seen.insert(address, different);
        different
    }

    /// Whether a rank is running at the given URL; any error means not
    /// running, an authorization failure included.
    async fn is_rank_running(&self, rank_url: &Url) -> bool {
        match self.is_system_running(rank_url).await {
            Ok(running) => running,
            Err(e) => {
                // Could not determine it; treat as unreachable, which is the
                // safe direction: it costs multi-head, not the failover.
                debug!("Rank address <{rank_url}> could not be probed: {e}");
                false
            }
        }
    }

    /// The address the cluster's head rank answers on, searched across every
    /// address advertised for it rather than only the one selection
    /// preferred. Tried in the order selection established, with addresses
    /// naming more than one host last. `None` if none answered.
    ///
    /// `server_is_local` is whether the address the connection was opened
    /// through names this machine. Where it does not, a candidate naming this
    /// machine is not probed, since a probe there answers whenever anything
    /// local is listening. Where it does, a candidate that answers but reports
    /// a different cluster is skipped, since then every address looks
    /// legitimate and only the cluster's own report tells a second instance on
    /// another port apart.
    async fn resolve_reachable_head_node_url(
        &self,
        system_properties: &HashMap<String, String>,
        selected: &Url,
        server_is_local: bool,
    ) -> Option<Url> {
        let retained = resolution::retain_matching_rank_candidate_urls(
            &resolution::extract_rank_candidate_urls(system_properties),
            self.hostname_regex.as_ref(),
        );
        let retained = match retained {
            Ok(retained) => {
                // The same credibility filter as the rank addresses
                let mut credible = Vec::with_capacity(retained.len());
                for candidates in retained {
                    credible.push(
                        resolution::retain_credible_urls(candidates, server_is_local, "head rank candidate").await,
                    );
                }
                Ok(credible)
            }
            Err(e) => Err(e),
        };
        let candidates = match retained {
            Ok(retained) => retained.first().cloned().flatten().map(|head_candidates| {
                let rank_host = resolution::extract_rank_host_mapping(system_properties, retained.len());
                resolution::order_urls_by_unique_hosts_first(
                    head_candidates,
                    &resolution::non_identifying_hosts(&retained, &rank_host),
                )
            }),
            Err(e) => {
                // Selection already produced `selected` from these same
                // properties, so a failure here means only that there are no
                // alternates to fall through.
                debug!("Could not re-derive head rank candidates: {e}");
                None
            }
        };

        let candidates = candidates.filter(|c| !c.is_empty()).unwrap_or_else(|| vec![selected.clone()]);

        // The identity comparison is spent only where the locality filter
        // abstains: where the server is elsewhere, an address naming this
        // machine has already been removed.
        let reference = server_is_local.then(|| resolution::cluster_identity_signature(system_properties));
        self.get_running_rank_url(&candidates, None, "Head rank", reference.as_deref(), &mut HashMap::new())
            .await
    }

    /// The current cluster's multi-head snapshot, or `None` where there is no
    /// current cluster. Multi-head objects keep the one they last built from
    /// and compare by pointer; a new one means the cluster's answer was
    /// replaced.
    pub(crate) fn current_multi_head_snapshot(&self) -> Option<Arc<MultiHeadSnapshot>> {
        self.cluster_info().map(|cluster| cluster.multi_head_snapshot().clone())
    }

    /// Records that the cluster now current has been newly arrived on and so
    /// owes a capability probe; called by every path that moves the
    /// connection to a different cluster.
    fn mark_arrival_owes_probe(&self) {
        if let Some(slot) = self.current_cluster_slot() {
            if let Some(cluster) = self.host_addresses.write().get_mut(slot) {
                cluster.set_probe_owed(true);
            }
        }
    }

    /// Answers with the current cluster's multi-head snapshot: the only way a
    /// multi-head object obtains rank addresses.
    ///
    /// The caller says what it observed and the connection decides what that
    /// costs. `topology_may_have_moved` is `true` where the caller saw
    /// something that can move rank addresses without moving the connection --
    /// a change of shard version -- and the cluster is probed again before
    /// answering. Otherwise the answer is what the connection holds, once this
    /// arrival's own probe has run: a cluster is probed once per arrival, not
    /// once per object.
    pub(crate) async fn acquire_multi_head_snapshot(&self, topology_may_have_moved: bool) -> Option<Arc<MultiHeadSnapshot>> {
        if topology_may_have_moved {
            self.probe_current_cluster_multi_head().await;
        } else {
            self.await_arrival_probe().await;
        }
        self.current_multi_head_snapshot()
    }

    /// Ensures this arrival's probe has happened before the caller reads
    /// addresses, without performing a second one. Usually the switching
    /// caller has already probed; otherwise this waits out its probe, or
    /// performs the one the arrival owes.
    async fn await_arrival_probe(&self) {
        if !self.cluster_info().is_some_and(|cluster| cluster.is_probe_owed()) {
            return;
        }
        let _probing = self.probe_lock.lock().await;
        if self.cluster_info().is_some_and(|cluster| cluster.is_probe_owed()) {
            self.probe_current_cluster_multi_head_locked().await;
        }
    }

    /// Whether multi-head operations can be used against the cluster currently
    /// being talked to.
    ///
    /// Two independent things have to hold: the user must not have disabled
    /// auto-discovery (a connection-wide choice), and the current cluster must
    /// actually have given us usable worker addresses (an observation about
    /// that one cluster).
    pub fn is_multi_head_available(&self) -> bool {
        if *self.disable_auto_discovery.read() {
            return false;
        }

        // No cluster known yet is not a reason to declare multi-head
        // unavailable; nothing has observed otherwise.
        self.cluster_info()
            .map(|cluster| cluster.is_multi_head_available())
            .unwrap_or(true)
    }

    /// Gets the protocol (http or https) of the current URL.
    ///
    /// # Returns
    ///
    /// The protocol scheme of the current URL (e.g., "http" or "https").
    pub fn url_protocol(&self) -> &str {
        // The cluster in use now, not `self.url`, which a failover leaves as is.
        let current = self.url().map(|u| u.scheme().to_string()).unwrap_or_else(|| self.url.clone());
        if current.starts_with("https") {
            "https"
        } else {
            "http"
        }
    }

    // ==================== HA Failover Methods ====================

    /// Gets the number of times the client has switched to a different cluster.
    pub fn num_cluster_switches(&self) -> usize {
        *self.num_cluster_switches.read()
    }

    /// Increments the cluster switch counter.
    fn increment_num_cluster_switches(&self) {
        let mut count = self.num_cluster_switches.write();
        *count += 1;
    }

    /// Gets the current cluster index pointer.
    fn curr_cluster_index_pointer(&self) -> usize {
        *self.current_cluster_index_pointer.read()
    }

    /// Sets the current cluster index pointer.
    fn set_curr_cluster_index_pointer(&self, new_index: usize) {
        let mut pointer = self.current_cluster_index_pointer.write();
        *pointer = new_index;
    }

    /// Selects the next cluster based on the HA failover priority.
    fn select_next_cluster(&self) {
        let curr_pointer = self.curr_cluster_index_pointer();
        let ring_size = self.ha_ring_size();

        debug!(
            "Cluster switch #{} from cluster #{} ({:?}) to the next one",
            self.num_cluster_switches() + 1,
            curr_pointer + 1,
            self.url()
        );

        // Increment the index by one (wrapping around at the list end)
        self.set_curr_cluster_index_pointer((curr_pointer + 1) % ring_size);

        // Keep a running count of how many times we had to switch clusters
        self.increment_num_cluster_switches();

        debug!(
            "Cluster switch #{} to cluster #{} ({:?})",
            self.num_cluster_switches(),
            self.curr_cluster_index_pointer() + 1,
            self.url()
        );

        // Announce a switch that lands on a cluster which cannot give this
        // client multi-head. This is the failover counterpart of the notice at
        // the end of process_cluster_information_for_all_urls(), which covers
        // the cluster first connected to: capability is per cluster, so a
        // failover can silently reduce a connection that was running multi-head
        // to head-node-only.
        //
        // Skipped while the cluster still owes a probe: "not available" is then
        // only "not yet established", and a ring walk would otherwise warn about
        // every cluster it passed through.
        if let Some(new_cluster) = self.cluster_info() {
            if !new_cluster.is_probe_owed() && !new_cluster.is_multi_head_available() {
                warn!(
                    "Failed over to a cluster with no multi-head available <{}>: \
                     its worker addresses are not reachable from this client. \
                     Operations will use the head node only.",
                    new_cluster.active_head_node_url()
                );
            }
        }
    }

    /// Checks if Kinetica is running at the given URL.
    ///
    /// Uses a quick ping, bounded by [`FAST_CONNECTION_TIMEOUT`], to verify
    /// server availability.
    pub async fn is_kinetica_running(&self, url: &Url) -> bool {
        is_kinetica_running_at(&self.hyper_client, &self.headers, url).await
    }

    /// Starts the fail-back poller after a failover has moved the connection
    /// off the primary cluster: only when there is a primary and more than one
    /// cluster, and not when a poller is already running.
    fn start_failback_poller_if_needed(&self) {
        if self.primary_url_hostname.read().is_empty() || self.ha_ring_size() <= 1 {
            return;
        }
        // The primary is kept at index 0 of the ring (see randomize_urls).
        let Some(primary) = self.host_addresses.read().first().map(|c| c.active_head_node_url().clone()) else {
            return;
        };

        let mut poller = self.failback_poller.lock();
        if poller.as_ref().is_some_and(|p| p.is_running()) {
            return;
        }
        let access = crate::failback::PrimaryAccess {
            client: self.hyper_client.clone(),
            headers: self.headers.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            bypass_ssl_cert_check: self.options.bypass_ssl_cert_check,
            trust_store_file_path: self.options.trust_store_file_path.clone(),
            trust_store_password: self.options.trust_store_password.clone(),
        };
        *poller = Some(crate::failback::FailbackPoller::start(
            primary,
            access,
            // 0 polls again at once
            std::time::Duration::from_secs(self.options.failback_polling_interval),
            crate::failback::FailbackTarget {
                host_addresses: self.host_addresses.clone(),
                ha_url_indices: self.ha_url_indices.clone(),
                current_cluster_index_pointer: self.current_cluster_index_pointer.clone(),
                switch_lock: self.switch_lock.clone(),
            },
        ));
    }

    /// Switches to the next available cluster URL for HA failover.
    ///
    /// This method:
    /// - Checks if failover is disabled
    /// - Checks if only one cluster exists (no failover possible)
    /// - Checks if another thread has already switched
    /// - Selects the next available cluster
    /// - Skips clusters that aren't running
    ///
    /// # Arguments
    ///
    /// * `old_url` - The URL that was in use when the failure occurred
    /// * `old_num_cluster_switches` - The switch count before this switch was initiated
    ///
    /// # Returns
    ///
    /// The new URL to use, or an error if failover is not possible.
    pub async fn switch_url(
        &self,
        old_url: &Url,
        old_num_cluster_switches: usize,
    ) -> Result<Url, KineticaError> {
        debug!(
            "Attempting to switch URLs, from: {:?}; originally failing URL: {}",
            self.url(),
            old_url
        );

        if self.disable_failover {
            debug!("Failover is disabled; returning the error");
            return Err(KineticaError::FailoverDisabled("Failover is disabled!".to_string()));
        }

        // Everything from here to the choice of cluster runs under one lock, so
        // concurrent failovers cannot read and advance the cluster index in
        // between each other and skip a cluster.
        let guard = self.switch_lock.lock().await;

        // If there is only one URL, then we can't switch URLs
        let ring_size = self.ha_ring_size();
        if ring_size == 1 {
            debug!("Only one cluster in ring--no fail-over cluster available");
            return Err(KineticaError::HaUnavailable(
                "Only one cluster in ring; HA failover unavailable".to_string(),
            ));
        }

        // Get how many more times other threads have switched clusters
        // since the caller called this function.
        //
        // count = 0             -> the calling thread is the first to get here; switch to the next cluster
        // 0 < count < ring size -> another thread is either in the process of switching or has switched
        // count >= ring size    -> another thread has already tried all failover clusters; throw exception
        let count_switches_since_invocation =
            self.num_cluster_switches().saturating_sub(old_num_cluster_switches);

        debug!(
            "Cluster fail-over attempts across all threads vs. total clusters in ring: {count_switches_since_invocation} vs. {ring_size}"
        );

        // Check if another thread has tried all the clusters in the HA ring
        if count_switches_since_invocation >= ring_size {
            return Err(KineticaError::HaUnavailable(format!(
                "Fail-over attempted as many times as clusters in the ring; URLs attempted: {}",
                url_list(&self.urls())
            )));
        }

        // Check if another thread beat us to switching the URL. Adopt its
        // choice only once it is known to be usable: the switch it made may be
        // old, or may have been a circle-back that left the index on a cluster
        // just found unusable.
        if let Some(current_url) = self.url() {
            if current_url != *old_url && count_switches_since_invocation > 0 {
                if self.is_cluster_usable(&current_url, true).await {
                    debug!("Already failed over to URL: {current_url}");
                    return Ok(current_url);
                }
                debug!("Already failed over to URL: {current_url}, but it is not usable; continuing to switch");
            }
        }

        // This thread is the first one here--select the next cluster to use
        let switched_to = loop {
            self.select_next_cluster();

            let Some(current_url) = self.url() else {
                return Err(KineticaError::HaUnavailable(
                    "No current URL available".to_string(),
                ));
            };

            // If we've circled back, give up
            if current_url == *old_url {
                debug!(
                    "Current URL is the same as the original URL: {old_url}; resetting to the primary and returning the error"
                );
                // Reset the ring walk to its starting position, index 0 (the
                // primary), so that a walk which gave up does not leave the
                // pointer parked where it stopped and the next failover starts
                // from the primary.
                self.set_curr_cluster_index_pointer(0);
                self.mark_arrival_owes_probe();

                return Err(KineticaError::HaUnavailable(format!(
                    "Circled back to original URL; no clusters available for fail-over among these: {}",
                    url_list(&self.urls())
                )));
            }

            // Accept the cluster only if it is running and not draining its HA
            // queue, not merely answering a ping.
            if self.is_cluster_usable(&current_url, true).await {
                break current_url;
            }
        };

        // Haven't circled back to the old URL; so keep the new one
        warn!("Switched to fail-over URL: {switched_to}");
        self.start_failback_poller_if_needed();

        // Mark the arrival before releasing the lock. The probe below runs
        // outside it, so a caller piggybacking on this switch can return and
        // ask for addresses first; the marker makes it wait for this arrival's
        // probe instead of reading the last visit's addresses.
        self.mark_arrival_owes_probe();
        drop(guard);

        // Establish multi-head on the cluster just switched to, once per
        // switch rather than once per multi-head object, and outside the
        // switch lock.
        self.probe_current_cluster_multi_head().await;

        Ok(switched_to)
    }

    // ==================== Request Submission Methods ====================

    /// Submits a request to the Kinetica server with HA failover support.
    ///
    /// This method implements HA failover:
    /// - Captures the current cluster switch count before attempting the request
    /// - On connection errors, attempts to switch to the next available cluster
    /// - Retries the request on the new cluster
    /// - Propagates API errors (server-side errors) without failover
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The API endpoint (e.g., "/show/table")
    /// * `req_schema` - Pre-parsed Avro schema for the request
    /// * `resp_schema` - Pre-parsed Avro schema for the response
    /// * `request` - The request object
    ///
    /// # Returns
    ///
    /// The response object, or an error if the request fails on all clusters.
    pub async fn submit_request<Req, Resp>(
        &self,
        endpoint: &str,
        req_schema: &FastSchema,
        resp_schema: &FastSchema,
        request: &Req,
    ) -> Result<Resp, KineticaError>
    where
        Req: KineticaRecord,
        Resp: DeserializeOwned,
    {
        // Get the current URL to send the request to
        let mut url = self.url().ok_or_else(|| {
            KineticaError::Connection("No URL available".to_string())
        })?;
        let original_url = url.clone();

        loop {
            // Capture the current cluster switch count before attempting the request
            let current_cluster_switch_count = self.num_cluster_switches();

            // Build the full URL with the endpoint
            let full_url = append_path_to_url(&url, endpoint).to_string();

            // Attempt the request
            match self.submit_request_raw(&full_url, req_schema, resp_schema, request).await {
                Ok(response) => return Ok(response),

                Err(KineticaError::Api(msg)) => {
                    // API errors are server-side errors; don't failover
                    debug!("Got API error, propagating: {msg}");
                    return Err(KineticaError::Api(msg));
                }

                // A server exiting or a request that could not be sent, or one
                // that failed on the way: fail over. Note what that implies for
                // writes: an Http error can come after the server has already
                // applied the request (the body failed to arrive, say), so a
                // non-idempotent request such as /insert/records may be applied
                // twice, once on each cluster.
                Err(error @ (KineticaError::Exit(_) | KineticaError::Connection(_) | KineticaError::Http(_))) => {
                    let kind = if matches!(error, KineticaError::Http(_)) { "an HTTP" } else { "an exit" };
                    warn!(
                        "Got {kind} error when trying endpoint {endpoint} at {url}: {}; switch URL...",
                        error.message()
                    );
                    match self.switch_url(&original_url, current_cluster_switch_count).await {
                        Ok(new_url) => {
                            debug!("Switched to {new_url}");
                            url = new_url;
                        }
                        Err(KineticaError::HaUnavailable(ha_msg) | KineticaError::FailoverDisabled(ha_msg)) => {
                            return Err(failover_exhausted(&error, &ha_msg));
                        }
                        Err(e) => return Err(e),
                    }
                }

                Err(e) => {
                    // Other errors (Avro, Config, etc.) - don't failover
                    return Err(e);
                }
            }
        }
    }

    /// Whether a payload of `request_size` bytes can be Snappy-compressed: one
    /// over [`MAX_SNAPPY_COMPRESSION_SIZE`] cannot, and is sent uncompressed,
    /// with a warning the first time and a debug message after that.
    fn is_snappy_compressible(&self, request_size: usize) -> bool {
        if request_size <= MAX_SNAPPY_COMPRESSION_SIZE {
            return true;
        }

        if !self
            .did_snappy_compression_size_warning
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            warn!(
                "Snappy compression enabled, but data size <{request_size}> exceeds compressible \
                 limit <{MAX_SNAPPY_COMPRESSION_SIZE}>.  This and any future data above the limit \
                 will be sent uncompressed."
            );
        } else {
            debug!(
                "Snappy compression enabled, but data size <{request_size}> exceeds compressible \
                 limit <{MAX_SNAPPY_COMPRESSION_SIZE}>.  This data will be sent uncompressed."
            );
        }
        false
    }

    /// Prepares the encoded body of a `Req` for sending, as
    /// [`Self::submit_request_raw`] would: Snappy-compressed when the endpoint
    /// opts in, `use_snappy` is set and the payload is small enough, and as it
    /// is otherwise. Returns the body and whether it was compressed.
    ///
    /// CPU-bound for a large body, so a caller with one may run it off the
    /// async runtime, then send the result with
    /// [`Self::submit_encoded_request_raw`].
    pub(crate) fn compress_request_body<Req: KineticaRecord>(
        &self,
        body: Vec<u8>,
    ) -> Result<(hyper::body::Bytes, bool), KineticaError> {
        if Req::use_compression() && self.use_snappy && self.is_snappy_compressible(body.len()) {
            Ok((crate::hyper_client::snappy_compress(&body)?.into(), true))
        } else {
            Ok((body.into(), false))
        }
    }

    /// Sends a request body that is already encoded, and compressed if
    /// `snappy`, directly to `url` without HA failover, and reads the response
    /// as [`Self::submit_request_raw`] does.
    pub(crate) async fn submit_encoded_request_raw<Resp: DeserializeOwned>(
        &self,
        url: &str,
        body: hyper::body::Bytes,
        snappy: bool,
        resp_schema: &FastSchema,
    ) -> Result<Resp, KineticaError> {
        trace!("Submitting request to {url}; payload size: {} bytes (snappy={snappy})", body.len());
        let resp = self.hyper_client.post_encoded_raw(url, body, snappy, &self.request_headers()).await?;
        self.read_response(url, &resp, resp_schema)
    }

    /// Submits a request to the host manager of the cluster in use: once, with
    /// no fail-over.
    ///
    /// The generated host manager endpoints (`/admin/add/host`,
    /// `/admin/alter/host`, `/admin/remove/host`, `/admin/show/alerts` and
    /// `/admin/switchover`) come through here. They name hosts and ranks within
    /// the cluster they are sent to, so a failure goes back to the caller
    /// rather than replaying the operation on another cluster.
    pub async fn submit_request_to_hm<Req, Resp>(
        &self,
        endpoint: &str,
        req_schema: &FastSchema,
        resp_schema: &FastSchema,
        request: &Req,
    ) -> Result<Resp, KineticaError>
    where
        Req: KineticaRecord,
        Resp: DeserializeOwned,
    {
        let hm_url = self.hm_url().ok_or_else(|| {
            KineticaError::Connection("No URL available".to_string())
        })?;
        let full_url = append_path_to_url(&hm_url, endpoint).to_string();
        self.submit_request_raw(&full_url, req_schema, resp_schema, request).await
    }

    /// Submits a request directly to a specific URL without HA failover.
    ///
    /// This is the low-level method used internally by `submit_request` and
    /// by bulk inserters that handle their own routing.
    ///
    /// # Arguments
    ///
    /// * `url` - Full URL including endpoint
    /// * `req_schema` - Pre-parsed Avro schema for the request
    /// * `resp_schema` - Pre-parsed Avro schema for the response
    /// * `request` - The request object
    ///
    /// # Performance
    ///
    /// This is the fastest method, saving ~5-10ms per request by avoiding
    /// all schema parsing. Critical for bulk insert performance.
    ///
    /// # Compression
    ///
    /// The payload is Snappy-compressed only when `options.use_snappy` is true
    /// *and* the request type opts in via [`KineticaRecord::use_compression`],
    /// which the generator enables for `/insert/records` and `/update/records`
    /// alone. Compressing a two-field `has_table` request costs more than it
    /// saves.
    ///
    /// Payloads above [`MAX_SNAPPY_COMPRESSION_SIZE`] are sent uncompressed.
    pub async fn submit_request_raw<Req, Resp>(
        &self,
        url: &str,
        req_schema: &FastSchema,
        resp_schema: &FastSchema,
        request: &Req
    ) -> Result<Resp, KineticaError>
    where Req: KineticaRecord + Serialize, Resp: DeserializeOwned {
        trace!("Submitting request to {url}");

        // Encode request to Avro using serde_avro_fast
        let mut config = serde_avro_fast::ser::SerializerConfig::new(req_schema);
        let req_bytes = serde_avro_fast::to_datum(request, Vec::new(), &mut config)
            .map_err(|e| KineticaError::Avro(format!("Request encode failed: {e}")))?;

        // Compression is opt-in per endpoint (see `KineticaRecord::use_compression`),
        // gated by the client-wide switch and skipped for payloads Snappy cannot
        // take. Oversized payloads go out uncompressed rather than failing.
        let use_snappy = Req::use_compression()
            && self.use_snappy
            && self.is_snappy_compressible(req_bytes.len());

        trace!("Request payload size: {} bytes (snappy={})", req_bytes.len(), use_snappy);

        // Use hyper client to POST (with optional Snappy compression)
        let resp = self.hyper_client.post_binary_raw(
            url,
            req_bytes,
            &self.request_headers(),
            use_snappy,
        ).await?;

        self.read_response(url, &resp, resp_schema)
    }
}

impl Kinetica {
    /// Reads a response: classifies it (see [`interpret_response`]), then
    /// decodes its payload with the pre-parsed response schema. A payload
    /// that does not decode is a submit failure, so it fails over like a
    /// wrapper that does not. A decoded response is checked for the session
    /// being used (see [`Self::note_session_used`]).
    fn read_response<Resp: DeserializeOwned>(
        &self,
        url: &str,
        resp: &crate::hyper_client::RawResponse,
        resp_schema: &FastSchema,
    ) -> Result<Resp, KineticaError> {
        let wrapper = interpret_response(url, resp)?;
        let decoded = serde_avro_fast::from_datum_slice::<Resp>(&wrapper.data, resp_schema)
            .map_err(|e| KineticaError::Http(format!("Payload decode failed: {e}")))?;
        self.note_session_used(&wrapper.data, resp_schema);
        Ok(decoded)
    }

    /// Notes whether a response reports this connection's session as used,
    /// in which case [`Self::close`] (or dropping the connection) must drop
    /// it. Once armed, it stays armed: a spurious drop costs one round trip, a
    /// missed one strands tables until the server reclaims them.
    ///
    /// Arming prepares the request as it stands: to the head node in use now,
    /// with the headers of the moment, the session's among them. A session the
    /// caller gave is never armed, as it is the caller's to drop.
    fn note_session_used(&self, data: &[u8], resp_schema: &FastSchema) {
        if !self.session.owned || self.session.is_armed() || !reports_session_used(data, resp_schema) {
            return;
        }

        let prepared = (|| {
            let url = self.url().ok_or_else(|| KineticaError::Connection("No URL available".to_string()))?;
            let request = crate::DropSessionRequest { options: HashMap::new() };
            let mut config = serde_avro_fast::ser::SerializerConfig::new(&crate::generated::DROP_SESSION_REQUEST_SCHEMA);
            let body = serde_avro_fast::to_datum(&request, Vec::new(), &mut config)
                .map_err(|e| KineticaError::Avro(format!("Request encode failed: {e}")))?;
            Ok::<_, KineticaError>(ArmedDrop {
                url: append_path_to_url(&url, "/drop/session").to_string(),
                headers: self.request_headers(),
                body: body.into(),
            })
        })();

        match prepared {
            Ok(drop) => self.session.arm(drop),
            Err(e) => warn!("Unable to prepare the drop of session <{}>: {}", self.session.id, e.message()),
        }
    }

    /// Closes this connection: drops the SQL `#name` temp tables it created,
    /// if any, and stops it sending requests. Closing a closed connection does
    /// nothing.
    ///
    /// The drop goes through [`Self::drop_session`], failing over as any
    /// request does; a failure is logged, not returned, since the server
    /// reclaims the session on its own. It is skipped where no temp table was
    /// created, and against a server that does not support session temp
    /// tables, which never reports one. A session given by
    /// [`KineticaOptions::session_id`] is not dropped: the caller drops it with
    /// [`Self::drop_session`].
    ///
    /// After this every request fails, at once and without failing over. The
    /// fail-back poller is stopped. A connection that is never closed has its
    /// session dropped when it is dropped, on a best-effort basis; closing it
    /// is what makes the drop prompt.
    pub async fn close(&self) {
        if self.closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }

        if self.session.is_armed() {
            if let Err(e) = self.drop_session().await {
                warn!("Unable to drop session <{}>: {}", self.session.id, e.message());
            }
        }
        self.session.disarm();

        *self.failback_poller.lock() = None;
        self.hyper_client.close();
    }

    /// Whether [`Self::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Drops the session of a connection dropped without being closed: one
/// best-effort POST of the prepared request, within [`SESSION_DROP_TIMEOUT`],
/// with no fail-over and the connection's own TLS trust. A failure is logged
/// at debug; the server reclaims the session on its own.
///
/// The POST is sent from a task on the async runtime the connection is
/// dropped on, so it runs after the drop. Dropped outside a runtime, or in
/// one that is shutting down, it is not sent.
impl Drop for Kinetica {
    fn drop(&mut self) {
        if !self.session_cleanup_registered {
            return;
        }
        let Some(drop) = self.session.disarm() else {
            return;
        };

        let id = self.session.id.clone();
        let client = self.hyper_client.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(e) = client.post_encoded_once(&drop.url, drop.body, &drop.headers, SESSION_DROP_TIMEOUT).await {
                        debug!("Unable to drop session <{id}> at collection: {}", e.message());
                    }
                });
            }
            Err(e) => debug!("Unable to drop session <{id}> at collection: {e}"),
        }
    }
}

#[cfg(test)]
mod compression_policy_tests {
    use crate::KineticaRecord;

    // Compression is opt-in per endpoint, for exactly these two. Widening the
    // set silently changes the Content-Type of live traffic, so pin it here
    // rather than leaving it to the generator alone.
    #[test]
    fn only_bulk_payload_endpoints_compress() {
        assert!(crate::RawInsertRecordsRequest::use_compression());
        assert!(crate::RawUpdateRecordsRequest::use_compression());
    }

    #[test]
    fn ordinary_endpoints_do_not_compress() {
        assert!(!crate::ClearTableRequest::use_compression());
        assert!(!crate::ShowTableRequest::use_compression());
        assert!(!crate::HasTableRequest::use_compression());
        // Discovery runs before anything is known about the server.
        assert!(!crate::ShowSystemPropertiesRequest::use_compression());
        assert!(!crate::ShowSystemStatusRequest::use_compression());
    }

    // The `/insert/records/*` family names its data rather than carrying it,
    // and none of them is compressed.
    #[test]
    fn insert_records_variants_do_not_compress() {
        assert!(!crate::InsertRecordsFromFilesRequest::use_compression());
        assert!(!crate::InsertRecordsFromPayloadRequest::use_compression());
        assert!(!crate::InsertRecordsFromQueryRequest::use_compression());
        assert!(!crate::InsertRecordsRandomRequest::use_compression());
    }
}

/// A removed rank contributes no hostname, and must not be mistaken for one.
#[cfg(test)]
mod removed_rank_hostname_tests {
    use super::*;

    #[test]
    fn cluster_hostnames_skip_removed_ranks() {
        let head = Url::parse("http://head:9191").unwrap();
        let info = ClusterAddressInfo::new(
            head.clone(),
            HashMap::new(),
            vec![
                Some(Url::parse("http://w1:9191").unwrap()),
                None,
                Some(Url::parse("http://w3:9191").unwrap()),
            ],
            HashSet::new(),
            head,
        );

        assert!(info.does_cluster_contain_node("head"));
        assert!(info.does_cluster_contain_node("w1"));
        assert!(info.does_cluster_contain_node("w3"));
        // Three ranks but only two live hosts, plus the head node.
        assert_eq!(info.host_names().len(), 3);
        assert_eq!(info.live_worker_rank_urls().count(), 2);
        assert_eq!(info.worker_rank_urls().len(), 3);
    }
}

/// Per-cluster multi-head capability.
///
/// Distinct from the connection-wide `disable_auto_discovery` option: this
/// records what was observed for one cluster, so a cluster this client cannot
/// reach the workers of does not cost the other clusters in the ring their
/// multi-head operations.
#[cfg(test)]
mod multi_head_availability_tests {
    use super::*;

    /// A discovered cluster, with one worker.
    fn cluster(url: &str) -> ClusterAddressInfo {
        let parsed = Url::parse(url).unwrap();
        ClusterAddressInfo::new(
            parsed.clone(),
            HashMap::new(),
            vec![Some(Url::parse("http://worker:9192").unwrap())],
            HashSet::new(),
            parsed,
        )
    }

    #[test]
    fn a_discovered_cluster_is_assumed_capable_until_observed_otherwise() {
        assert!(cluster("http://head:9191").is_multi_head_available());
    }

    // A bare cluster -- down at discovery, unreadable, or built with
    // auto-discovery off -- knows nothing of its workers, so it claims no
    // multi-head until a probe says otherwise.
    #[test]
    fn a_bare_cluster_claims_no_multi_head_until_probed() {
        let bare = ClusterAddressInfo::with_host_manager_port(Url::parse("http://head:9191").unwrap(), 9300).unwrap();
        assert!(!bare.is_multi_head_available());
        assert!(bare.is_probe_owed());
    }

    #[test]
    fn every_change_of_addresses_or_verdict_is_a_new_snapshot() {
        let mut info = cluster("http://head:9191");
        let before = info.multi_head_snapshot().clone();
        info.set_multi_head_available(false);
        assert!(!Arc::ptr_eq(&before, info.multi_head_snapshot()));
        let clone = info.clone();
        assert!(Arc::ptr_eq(clone.multi_head_snapshot(), info.multi_head_snapshot()), "a copy is the same answer");
    }

    #[test]
    fn a_cluster_can_be_recorded_as_degraded() {
        let mut info = cluster("http://head:9191");
        info.set_multi_head_available(false);
        assert!(!info.is_multi_head_available());
    }

    /// The capability belongs to the cluster, not the connection: marking one
    /// degraded must leave the others alone. This is the invariant that
    /// KECO-2692 restored — one unreachable cluster used to disable
    /// auto-discovery for the entire ring.
    #[test]
    fn degrading_one_cluster_leaves_the_others_capable() {
        let mut ring = [
            cluster("http://c1:9191"),
            cluster("http://c2:9191"),
            cluster("http://c3:9191"),
        ];

        ring[1].set_multi_head_available(false);

        assert!(ring[0].is_multi_head_available());
        assert!(!ring[1].is_multi_head_available());
        assert!(ring[2].is_multi_head_available());
    }

    /// A degraded cluster keeps its head node address, which is the whole point:
    /// it is still usable, just not for multi-head.
    #[test]
    fn a_degraded_cluster_is_still_addressable() {
        let mut info = cluster("http://head:9191");
        info.set_multi_head_available(false);

        assert_eq!(info.active_head_node_url().host_str(), Some("head"));
        assert!(info.does_cluster_contain_node("head"));
    }
}

#[cfg(test)]
mod response_classification_tests {
    use super::*;
    use crate::hyper_client::RawResponse;
    use hyper::body::Bytes;

    #[derive(serde::Serialize)]
    struct Wrapper<'a> {
        status: &'a str,
        message: &'a str,
        schema: &'a str,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    }

    fn avro(status: &str, message: &str) -> Bytes {
        let mut config = serde_avro_fast::ser::SerializerConfig::new(wrapper_schema());
        let w = Wrapper { status, message, schema: "", data: Vec::new() };
        Bytes::from(serde_avro_fast::to_datum(&w, Vec::new(), &mut config).unwrap())
    }

    fn response(status: u16, content_type: Option<&str>, body: Bytes) -> RawResponse {
        RawResponse {
            status,
            reason: hyper::StatusCode::from_u16(status).unwrap().canonical_reason().unwrap_or("").to_string(),
            content_type: content_type.map(str::to_string),
            body,
        }
    }

    fn classify(status: u16, content_type: Option<&str>, body: Bytes) -> Result<WrapperResponse, KineticaError> {
        interpret_response("http://h:9191/x", &response(status, content_type, body))
    }

    const OCTET: Option<&str> = Some("application/octet-stream");

    #[test]
    fn a_successful_response_is_returned() {
        let w = classify(200, OCTET, avro("OK", "")).unwrap();
        assert_eq!(w.status, "OK");
    }

    #[test]
    fn unauthorized_and_forbidden_do_not_fail_over() {
        for status in [401, 403] {
            let err = classify(status, OCTET, avro("ERROR", "nope")).unwrap_err();
            assert!(matches!(err, KineticaError::Unauthorized(_)), "{status}: {err:?}");
        }
    }

    #[test]
    fn an_ordinary_server_error_does_not_fail_over() {
        // Including one sent with a 4xx status: before, any non-2xx failed over.
        for status in [200, 400, 404] {
            let err = classify(status, OCTET, avro("ERROR", "Table does not exist")).unwrap_err();
            assert!(matches!(err, KineticaError::Api(ref m) if m == "Table does not exist"), "{status}: {err:?}");
        }
    }

    // A server error saying the server sent no response fails over, as an
    // exit with the same message.
    #[test]
    fn a_server_error_saying_no_response_came_is_an_exit() {
        for status in [200, 400] {
            let message = "Error on worker: Unexpected end of file from server";
            let err = classify(status, OCTET, avro("ERROR", message)).unwrap_err();
            assert!(matches!(err, KineticaError::Exit(ref m) if m == message), "{status}: {err:?}");
        }
    }

    #[test]
    fn a_server_that_cannot_serve_is_an_exit() {
        for message in [
            "Kinetica is exiting",
            "Connection refused",
            "Connection reset by peer",
            "Error: system-limited-fatal",
            "Kinetica is offline",
            "Query planner is not running",
        ] {
            let err = classify(200, OCTET, avro("ERROR", message)).unwrap_err();
            assert!(matches!(err, KineticaError::Exit(_)), "{message}: {err:?}");
        }
    }

    #[test]
    fn server_and_gateway_error_statuses_are_exits() {
        for status in [500, 504] {
            let err = classify(status, OCTET, avro("ERROR", "boom")).unwrap_err();
            assert!(matches!(err, KineticaError::Exit(ref m) if m == "boom"), "{status}: {err:?}");
        }
        for status in [502, 503] {
            let err = classify(status, OCTET, avro("ERROR", "boom")).unwrap_err();
            assert!(
                matches!(err, KineticaError::Exit(ref m) if m.contains("stopped or suspended")),
                "{status}: {err:?}"
            );
        }
    }

    #[test]
    fn a_shutting_down_or_draining_bad_request_is_an_exit() {
        let err = classify(400, OCTET, avro("ERROR", "System shutting down")).unwrap_err();
        assert!(matches!(err, KineticaError::Exit(ref m) if m == "Kinetica shutting down"), "{err:?}");
        let err = classify(400, OCTET, avro("ERROR", "Unavailable: Draining HA queue")).unwrap_err();
        assert!(matches!(err, KineticaError::Exit(ref m) if m == "Draining HA message queue"), "{err:?}");
    }

    #[test]
    fn a_text_body_is_an_exit_on_server_errors_and_a_submit_failure_otherwise() {
        let html = Bytes::from_static(b"<html>Bad Gateway</html>");
        for status in [500, 502, 503, 504] {
            let err = classify(status, Some("text/html"), html.clone()).unwrap_err();
            assert!(matches!(err, KineticaError::Exit(_)), "{status}: {err:?}");
        }
        let err = classify(404, Some("text/html"), html).unwrap_err();
        assert!(matches!(err, KineticaError::Http(ref m) if m.contains("status code: 404")), "{err:?}");
    }

    #[test]
    fn an_empty_body_is_an_exit() {
        let err = classify(200, OCTET, Bytes::new()).unwrap_err();
        assert!(matches!(err, KineticaError::Exit(_)), "{err:?}");
    }

    #[test]
    fn an_undecodable_body_is_a_submit_failure() {
        let err = classify(200, OCTET, Bytes::from_static(&[0xff, 0xff, 0xff])).unwrap_err();
        assert!(matches!(err, KineticaError::Http(_)), "{err:?}");
    }
}

/// Switching back to the primary once the fail-back poller reports it ready.
#[cfg(test)]
mod failback_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// A client whose clusters are all unreachable local ports. Discovery
    /// fails, which the URL processing tolerates, leaving one minimal entry
    /// per URL: enough to exercise the ring pointer. Built without the
    /// connectivity check, which an unreachable cluster fails.
    ///
    /// The ports stay held, refusing connections, while the returned sockets
    /// live, so no other test can be given one and receive this client's
    /// requests.
    async fn unreachable_client(clusters: usize) -> (Kinetica, Vec<socket2::Socket>) {
        let (held, urls): (Vec<_>, Vec<_>) = (0..clusters)
            .map(|_| {
                let (socket, address) = crate::test_net::refusing_address();
                (socket, format!("http://{address}"))
            })
            .unzip();
        (Kinetica::connect(urls, KineticaOptions::default()).await.expect("client"), held)
    }

    #[tokio::test]
    async fn a_host_manager_request_goes_to_the_host_manager_and_does_not_fail_over() {
        use tokio::io::AsyncReadExt;

        let (k, _held) = unreachable_client(1).await;
        // A host manager that reads the request line and hangs up.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hm_url = Url::parse(&format!("http://{}/gpudb-host-manager", listener.local_addr().unwrap())).unwrap();
        k.host_addresses.write()[0].set_host_manager_url(hm_url.clone());
        let request_line = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or_default().to_string()
        });

        let switches = k.num_cluster_switches();
        let result = k.admin_show_alerts(10).await;

        assert_eq!(k.hm_url(), Some(hm_url));
        let request_line = tokio::time::timeout(std::time::Duration::from_secs(5), request_line)
            .await
            .expect("the request never reached the host manager")
            .unwrap();
        assert_eq!(request_line, "POST /gpudb-host-manager/admin/show/alerts HTTP/1.1");
        assert!(result.is_err(), "the host manager hung up");
        assert_eq!(k.num_cluster_switches(), switches, "no fail-over was attempted");
    }

    #[tokio::test]
    async fn a_payload_over_the_snappy_limit_is_warned_about_once() {
        let (k, _held) = unreachable_client(1).await;
        assert!(k.is_snappy_compressible(MAX_SNAPPY_COMPRESSION_SIZE));
        assert!(!k.did_snappy_compression_size_warning.load(Ordering::Relaxed));

        assert!(!k.is_snappy_compressible(MAX_SNAPPY_COMPRESSION_SIZE + 1));
        assert!(k.did_snappy_compression_size_warning.load(Ordering::Relaxed), "warned");
        // Later ones go to debug; the flag stays set.
        assert!(!k.is_snappy_compressible(MAX_SNAPPY_COMPRESSION_SIZE + 1));
        assert!(k.did_snappy_compression_size_warning.load(Ordering::Relaxed));
    }

    #[test]
    fn clusters_are_tried_in_order_by_default() {
        assert_eq!(KineticaOptions::default().ha_failover_order, HaFailoverOrder::Sequential);
    }

    /// The first cluster of each of `trials` random orders of a four-cluster ring.
    async fn first_clusters_tried(primary_hostname: &str, trials: usize) -> HashSet<usize> {
        let (mut k, _held) = unreachable_client(1).await;
        // Only the ring's length matters to the order, so repeat the cluster.
        let cluster = k.host_addresses.read()[0].clone();
        *k.host_addresses.write() = vec![cluster; 4];
        k.ha_failover_order = HaFailoverOrder::Random;
        *k.primary_url_hostname.write() = primary_hostname.to_string();
        (0..trials)
            .map(|_| {
                k.randomize_urls();
                let indices = k.ha_url_indices.read();
                let mut sorted = indices.clone();
                sorted.sort_unstable();
                assert_eq!(sorted, [0, 1, 2, 3], "a shuffle, not a selection");
                indices[0]
            })
            .collect()
    }

    #[tokio::test]
    async fn a_random_order_keeps_a_given_primary_first() {
        assert_eq!(first_clusters_tried("127.0.0.1", 50).await, HashSet::from([0]));
    }

    #[tokio::test]
    async fn a_random_order_without_a_primary_shuffles_every_cluster() {
        // The whole ring is shuffled when no primary was given; 200 trials
        // leave each of the four clusters first with near certainty.
        assert_eq!(first_clusters_tried("", 200).await.len(), 4);
    }

    // The poller switches the connection back through the cluster ring it
    // shares with it: the connection is on the primary at once, with no request
    // in between, and the primary owes a multi-head probe.
    #[tokio::test]
    async fn a_fail_back_moves_the_connection_at_once() {
        // Clusters are told apart by host, so each has an address of its own
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(200);
        let urls = vec!["http://127.0.0.236:1".to_string(), "http://127.0.0.237:1".to_string()];
        let k = Kinetica::connect(urls, options).await.expect("client");
        k.set_curr_cluster_index_pointer(1);
        let primary = k.host_addresses.read()[k.ha_url_indices.read()[0]].active_head_node_url().clone();
        assert_ne!(k.url(), Some(primary.clone()));
        let switches = k.num_cluster_switches();

        let target = crate::failback::FailbackTarget {
            host_addresses: k.host_addresses.clone(),
            ha_url_indices: k.ha_url_indices.clone(),
            current_cluster_index_pointer: k.current_cluster_index_pointer.clone(),
            switch_lock: k.switch_lock.clone(),
        };
        target.fail_back().await;

        assert_eq!(k.curr_cluster_index_pointer(), 0, "the primary is at index 0");
        assert_eq!(k.url(), Some(primary));
        assert!(k.cluster_info().unwrap().is_probe_owed(), "an arrival owes a probe");
        // A fail-back moves the pointer without spending anyone's attempt, so
        // an in-flight failover cannot be told the ring is exhausted when it is
        // not.
        assert_eq!(k.num_cluster_switches(), switches, "a fail-back leaves the counter alone");
    }

    #[test]
    fn the_default_polling_interval() {
        assert_eq!(KineticaOptions::default().failback_polling_interval, 5);
    }
}

/// Cluster switching against clusters that are all unreachable.
#[cfg(test)]
mod switch_tests {
    use super::*;

    /// A client over `clusters` distinct loopback hosts with nothing
    /// listening. Clusters are told apart by host, so each needs its own
    /// address, not just its own port. Built without the connectivity check,
    /// which nothing listening would fail.
    async fn unreachable_ring(clusters: u8) -> Kinetica {
        let urls = (0..clusters).map(|i| format!("http://127.0.0.{}:1", i + 2)).collect();
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(200);
        Kinetica::connect(urls, options).await.expect("client")
    }

    #[tokio::test]
    async fn a_client_cannot_be_built_against_an_unreachable_server() {
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(200).set_max_retries(0);
        let err = Kinetica::new(vec!["http://127.0.0.2:1".to_string()], options)
            .await
            .err()
            .expect("the constructor fails its connectivity check here");
        let text = err.to_string();
        assert!(text.starts_with("Connectivity check for <http://127.0.0.2:1/> failed: "), "{text}");
    }

    #[tokio::test]
    async fn an_unreachable_cluster_is_not_usable() {
        let k = unreachable_ring(1).await;
        let url = k.url().unwrap();
        assert!(!k.is_cluster_usable(&url, true).await);
    }

    #[tokio::test]
    async fn a_ring_with_no_usable_cluster_gives_up_and_resets_to_the_primary() {
        let k = unreachable_ring(3).await;
        assert_eq!(k.ha_ring_size(), 3);
        let url = k.url().unwrap();

        let err = k.switch_url(&url, k.num_cluster_switches()).await.unwrap_err();

        assert!(matches!(err, KineticaError::HaUnavailable(_)), "{err:?}");
        assert_eq!(k.curr_cluster_index_pointer(), 0, "the walk resets to the primary");
    }

    #[tokio::test]
    async fn concurrent_failovers_are_serialized() {
        let k = unreachable_ring(3).await;
        let url = k.url().unwrap();
        let switches = k.num_cluster_switches();

        let (a, b) = tokio::join!(k.switch_url(&url, switches), k.switch_url(&url, switches));

        // Whichever runs second sees the first's switches and gives up rather
        // than walking the ring again from a stale index.
        assert!(matches!(a, Err(KineticaError::HaUnavailable(_))), "{a:?}");
        assert!(matches!(b, Err(KineticaError::HaUnavailable(_))), "{b:?}");
        assert_eq!(k.curr_cluster_index_pointer(), 0);
        assert_eq!(k.num_cluster_switches(), switches + 3, "the ring was walked once, not twice");
    }

    #[test]
    fn debug_output_masks_the_password() {
        let mut options = KineticaOptions::default();
        options.set_username("admin").set_password("s3cret");
        let shown = format!("{options:?}");
        assert!(!shown.contains("s3cret"), "{shown}");
        assert!(shown.contains("password: <********>"), "{shown}");
        assert!(shown.contains("username: \"admin\""), "{shown}");

        let empty = format!("{:?}", KineticaOptions::default());
        assert!(empty.contains("password: \"\""), "{empty}");
    }
}

#[cfg(test)]
mod server_version_tests {
    use super::*;

    fn parsed(version: &str) -> Result<KineticaVersion, String> {
        let props = HashMap::from([(DATABASE_SERVER_VERSION_KEY.to_string(), version.to_string())]);
        parse_server_version(&props).map_err(|e| e.to_string())
    }

    #[test]
    fn the_version_is_read_from_the_system_properties() {
        let v = parsed("7.2.3.27.20261002123456").unwrap();
        assert_eq!((v.major(), v.minor(), v.revision(), v.abi_version(), v.build()), (7, 2, 3, 27, 20261002123456));
        assert_eq!(v.to_string(), "7.2.3.27");
        assert_eq!(v.full_version(), "7.2.3.27.20261002123456");
    }

    #[test]
    fn a_malformed_version_fails() {
        assert_eq!(
            parse_server_version(&HashMap::new()).unwrap_err().to_string(),
            "API error: System properties does not have any entry for the 'version.gpudb_core_version' key"
        );
        assert_eq!(
            parsed("7.2.3").unwrap_err(),
            "API error: Could not parse server version; error: Server version string in \
             /show/system/properties response malformed (expected at least four components): 7.2.3"
        );
        // The build is needed too: a fifth component, read as an i64.
        assert_eq!(
            parsed("7.2.3.27").unwrap_err(),
            "API error: Could not parse server version; error: no build number after the four version components"
        );
        assert_eq!(
            parsed("7.x.3.27.1").unwrap_err(),
            "API error: Could not parse server version; error: 'x' is not an i32 version component"
        );
        assert_eq!(
            parsed("7.2.3.27.2026.10").unwrap_err(),
            "API error: Could not parse server version; error: '2026.10' is not an i64 build number"
        );
    }

    #[test]
    fn versions_compare_by_every_component_the_build_last() {
        let a = KineticaVersion::with_build(7, 2, 3, 27, 1);
        let b = KineticaVersion::with_build(7, 2, 3, 27, 2);
        assert_ne!(a, b, "equality takes the build");
        assert_eq!(a, KineticaVersion::with_build(7, 2, 3, 27, 1));
        assert!(a < b);
        assert!(KineticaVersion::new(7, 2, 3, 28) > b, "the build comes last");
        assert!(KineticaVersion::with_build(7, 2, 3, 27, 99) < KineticaVersion::new(7, 2, 4, 0));
    }
}

/// A stand-in Kinetica server for tests: answers `/show/system/status` and
/// `/show/system/properties` with Avro-wrapped responses, from any loopback
/// address so that tests can stand up several distinct "hosts".
#[cfg(test)]
pub(crate) mod fake_server {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Answers a request the server does not answer itself, given its path
    /// and body: `Some(Ok(payload))` replies with an encoded response,
    /// `Some(Err(message))` with an error, and `None` with a 404.
    pub(crate) type Handler = Arc<dyn Fn(&str, &[u8]) -> Option<Result<Vec<u8>, String>> + Send + Sync>;

    /// Answers a request with a status line and a JSON body sent as they
    /// are, given its path (query included) and body; `None` leaves it to the
    /// other handlers.
    pub(crate) type JsonHandler = Arc<dyn Fn(&str, &[u8]) -> Option<(String, String)> + Send + Sync>;

    /// What a fake server answers, and what it has been asked.
    #[derive(Default)]
    pub(crate) struct Behavior {
        /// Answers with raw JSON, before anything else.
        pub(crate) json_handler: parking_lot::Mutex<Option<JsonHandler>>,
        /// Answers any other path.
        pub(crate) handler: parking_lot::Mutex<Option<Handler>>,
        /// The `status_map` of `/show/system/status`; `None` means running.
        pub(crate) status: parking_lot::Mutex<Option<HashMap<String, String>>>,
        /// The `property_map` of `/show/system/properties`.
        pub(crate) properties: parking_lot::Mutex<HashMap<String, String>>,
        /// Requests received, by path.
        pub(crate) requests: parking_lot::Mutex<HashMap<String, usize>>,
        /// The head of the last request received, by path.
        pub(crate) heads: parking_lot::Mutex<HashMap<String, String>>,
        pub(crate) total: AtomicUsize,
    }

    impl Behavior {
        pub(crate) fn count(&self, path: &str) -> usize {
            self.requests.lock().get(path).copied().unwrap_or(0)
        }
    }

    /// A running fake server.
    pub(crate) struct FakeServer {
        pub(crate) url: Url,
        pub(crate) behavior: Arc<Behavior>,
    }

    pub(crate) fn encode<T: serde::Serialize>(schema: &FastSchema, value: &T) -> Vec<u8> {
        let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
        serde_avro_fast::to_datum(value, Vec::new(), &mut config).unwrap()
    }

    fn maps_schema(first: &str) -> FastSchema {
        FastSchema::from_str(&format!(
            r#"{{"type":"record","name":"r","fields":[{{"name":"{first}","type":{{"type":"map","values":"string"}}}},{{"name":"info","type":{{"type":"map","values":"string"}}}}]}}"#
        ))
        .unwrap()
    }

    fn wrapped(reply: Result<Vec<u8>, String>) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct Wrapper<'a> {
            status: &'a str,
            message: &'a str,
            schema: &'a str,
            #[serde(with = "serde_bytes")]
            data: Vec<u8>,
        }
        let wrapper = match &reply {
            Ok(payload) => Wrapper { status: "OK", message: "", schema: "", data: payload.clone() },
            Err(message) => Wrapper { status: "ERROR", message, schema: "", data: Vec::new() },
        };
        encode(wrapper_schema(), &wrapper)
    }

    /// The `status_map` of a running system.
    pub(crate) fn running() -> HashMap<String, String> {
        HashMap::from([("system".to_string(), r#"{"status":"running"}"#.to_string())])
    }

    /// Runs a handler, which may block (a test holding a reply back sleeps in
    /// it). On a multi-threaded runtime the worker's queued tasks go to
    /// another thread meanwhile: the task it woke last waits in a slot no
    /// other worker takes from, and would otherwise wait out the handler.
    fn blocking<R>(handle: impl FnOnce() -> R) -> R {
        if tokio::runtime::Handle::current().runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
            tokio::task::block_in_place(handle)
        } else {
            handle()
        }
    }

    /// Starts a server on `host` (a loopback address such as `127.0.0.2`).
    pub(crate) async fn start(host: &str) -> FakeServer {
        let listener = tokio::net::TcpListener::bind(format!("{host}:0")).await.unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let behavior = Arc::new(Behavior::default());
        let served = behavior.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let behavior = served.clone();
                tokio::spawn(async move {
                    loop {
                        // Read one request: head, then a Content-Length body
                        let mut head = Vec::new();
                        let mut byte = [0u8; 1];
                        while !head.ends_with(b"\r\n\r\n") {
                            match stream.read(&mut byte).await {
                                Ok(1) => head.push(byte[0]),
                                _ => return,
                            }
                        }
                        let head = String::from_utf8_lossy(&head).to_string();
                        let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
                        let length = head
                            .lines()
                            .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().to_string()))
                            .and_then(|v| v.parse::<usize>().ok())
                            .unwrap_or(0);
                        let mut body = vec![0u8; length];
                        if stream.read_exact(&mut body).await.is_err() {
                            return;
                        }

                        *behavior.requests.lock().entry(path.clone()).or_default() += 1;
                        behavior.heads.lock().insert(path.clone(), head.clone());
                        behavior.total.fetch_add(1, Ordering::SeqCst);

                        let json_handler = behavior.json_handler.lock().clone();
                        if let Some((status_line, json)) = json_handler.and_then(|handle| blocking(|| handle(&path, &body))) {
                            let reply = format!(
                                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{json}",
                                json.len()
                            );
                            if stream.write_all(reply.as_bytes()).await.is_err() {
                                return;
                            }
                            continue;
                        }
                        let handler = behavior.handler.lock().clone();
                        let handled = handler.and_then(|handle| blocking(|| handle(&path, &body)));
                        let reply = if let Some(reply) = handled {
                            reply
                        } else if path.ends_with("/show/system/status") {
                            let status = behavior.status.lock().clone().unwrap_or_else(running);
                            #[derive(serde::Serialize)]
                            struct R {
                                status_map: HashMap<String, String>,
                                info: HashMap<String, String>,
                            }
                            Ok(encode(&maps_schema("status_map"), &R { status_map: status, info: HashMap::new() }))
                        } else if path.ends_with("/show/system/properties") {
                            #[derive(serde::Serialize)]
                            struct R {
                                property_map: HashMap<String, String>,
                                info: HashMap<String, String>,
                            }
                            let properties = behavior.properties.lock().clone();
                            Ok(encode(&maps_schema("property_map"), &R { property_map: properties, info: HashMap::new() }))
                        } else {
                            let reply = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
                            if stream.write_all(reply).await.is_err() {
                                return;
                            }
                            continue;
                        };
                        let body = wrapped(reply);
                        let reply_head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
                            body.len()
                        );
                        if stream.write_all(reply_head.as_bytes()).await.is_err() || stream.write_all(&body).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        FakeServer { url, behavior }
    }

    /// The URL of a server, without the trailing `/`, as properties carry it.
    pub(crate) fn address(server: &FakeServer) -> String {
        server.url.as_str().trim_end_matches('/').to_string()
    }

    /// Properties for a one-host cluster whose head rank is `head` and whose
    /// workers are `workers`, advertised as given.
    pub(crate) fn cluster_properties(head: &str, workers: &[&str]) -> HashMap<String, String> {
        let mut ranks = vec![head.to_string()];
        ranks.extend(workers.iter().map(|w| w.to_string()));
        HashMap::from([
            ("conf.enable_worker_http_servers".to_string(), "TRUE".to_string()),
            ("conf.worker_http_server_urls".to_string(), ranks.join(";")),
            ("conf.number_of_hosts".to_string(), "1".to_string()),
            ("conf.host0_public_urls".to_string(), head.to_string()),
            ("version.gpudb_core_version".to_string(), "7.2.3.27.20261002000000".to_string()),
        ])
    }
}

/// The headers sent with every request.
#[cfg(test)]
mod header_tests {
    use super::fake_server::{self, address, cluster_properties};
    use super::*;

    /// The value of `name` in a request head, if it was sent.
    fn header(head: &str, name: &str) -> Option<String> {
        head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim().to_string())
        })
    }

    // A connection logs nothing at INFO; it used to log two lines for every
    // connection, the fail-back poller's probes included.
    #[test]
    fn a_connection_logs_nothing_at_info() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let (connected, records) = crate::test_log::capture(|| {
            runtime.block_on(async {
                let server = fake_server::start("127.0.0.160").await;
                *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
                let mut options = KineticaOptions::default();
                options.set_server_connection_timeout(500).set_max_retries(0);
                Kinetica::new(vec![server.url.to_string()], options).await.is_ok()
            })
        });
        assert!(connected);
        let info: Vec<_> = records.iter().filter(|(level, _)| *level == log::Level::Info).collect();
        assert!(info.is_empty(), "{info:?}");
        assert!(
            records.contains(&(log::Level::Debug, "Initializing Kinetica client with 1 URL(s)".to_string())),
            "the construction's records are the ones seen"
        );
    }

    // On connecting, the caller's headers are sent, Basic credentials among
    // them are used, X-Kinetica-Group names the sync mode, the protected
    // headers are dropped, and the User-Agent names the client program first.
    // An X-Kinetica-Group header of the mode's wire value sets the mode, which
    // goes out as that value.
    #[tokio::test]
    async fn a_sync_mode_header_may_name_the_wire_value() {
        let server = fake_server::start("127.0.0.167").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).add_http_header("X-Kinetica-Group", "REPL_SYNC");
        let k = Kinetica::new(vec![server.url.to_string()], options).await.expect("client");
        assert_eq!(k.ha_sync_mode(), HaSynchronicityMode::Synchronous);
        let head = server.behavior.heads.lock()["/show/system/properties"].clone();
        assert_eq!(header(&head, "X-Kinetica-Group").as_deref(), Some("REPL_SYNC"));
    }

    #[tokio::test]
    async fn the_callers_headers_are_sent() {
        let server = fake_server::start("127.0.0.95").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let mut options = KineticaOptions::default();
        options
            .set_server_connection_timeout(500)
            .set_max_retries(0)
            .set_username("ignored")
            .set_client_info(Some("My App"), Some("1.0"))
            .add_http_header("X-Custom", "1")
            .add_http_header("Authorization", "Basic dXNlcjpwYXNz")
            .add_http_header("X-Kinetica-Group", "SYNCHRONOUS")
            .add_http_header("User-Agent", "fake")
            .add_http_header("Content-type", "text/plain");

        let k = Kinetica::new(vec![server.url.to_string()], options).await.expect("client");
        let head = server.behavior.heads.lock()["/show/system/properties"].clone();

        assert_eq!(header(&head, "X-Custom").as_deref(), Some("1"));
        assert_eq!(header(&head, "Authorization").as_deref(), Some("Basic dXNlcjpwYXNz"), "the header's credentials");
        assert_eq!(header(&head, "X-Kinetica-Group").as_deref(), Some("REPL_SYNC"));
        assert_eq!(header(&head, "Content-type").as_deref(), Some("application/octet-stream"));
        let user_agent = header(&head, "User-Agent").unwrap();
        let expected = format!("My_App/1.0 kinetica-api-rust/{} (Rust/", env!("GPUDB_API_VERSION"));
        assert!(user_agent.starts_with(&expected), "{user_agent}");
        assert_eq!(k.user_agent().as_deref(), Some(user_agent.as_str()));
        assert_eq!(k.client_name(), Some("My App"));
        assert_eq!(k.ha_sync_mode(), HaSynchronicityMode::Synchronous);

        assert_eq!((k.username(), k.password()), ("user", "pass"), "the header's credentials");
        assert_eq!(k.primary_url(), Some(server.url.clone()), "a lone cluster is the primary");
        assert_eq!(k.primary_hostname(), "127.0.0.95");
        assert_eq!(k.system_properties().get("conf.number_of_hosts").map(String::as_str), Some("1"));
        // The version, then the time of the build
        let (version, build) = Kinetica::api_version().split_once('-').expect("a build number");
        // The crate version, MAJOR.MINOR.REVISION-ABI, as MAJOR.MINOR.REVISION.ABI
        let crate_version = concat!(
            env!("CARGO_PKG_VERSION_MAJOR"), ".", env!("CARGO_PKG_VERSION_MINOR"), ".",
            env!("CARGO_PKG_VERSION_PATCH"), ".", env!("CARGO_PKG_VERSION_PRE")
        );
        assert_eq!(version, crate_version);
        assert_eq!(env!("GPUDB_API_VERSION"), crate_version, "the User-Agent's version");
        assert!(build.len() == 14 && build.bytes().all(|b| b.is_ascii_digit()), "%Y%m%d%H%M%S: {build}");

        let headers = k.http_headers();
        let mut names: Vec<&str> = headers.keys().map(String::as_str).collect();
        names.sort_unstable();
        assert_eq!(names, ["User-Agent", "X-Custom"]);

        // Changing headers at run time, except for the protected ones
        assert_eq!(
            k.add_http_header("User-Agent", "x").unwrap_err().to_string(),
            "API error: Not allowed to change protected header: User-Agent"
        );
        assert_eq!(
            k.remove_http_header("Authorization").unwrap_err().to_string(),
            "API error: Not allowed to remove protected header: Authorization"
        );
        k.remove_http_header("X-Custom").unwrap();
        k.add_http_header("X-Other", "2").unwrap();
        k.set_ha_sync_mode(HaSynchronicityMode::Default);
        let _ = k.show_system_properties().await.unwrap();
        let head = server.behavior.heads.lock()["/show/system/properties"].clone();
        assert_eq!(header(&head, "X-Custom"), None);
        assert_eq!(header(&head, "X-Other").as_deref(), Some("2"));
        assert_eq!(header(&head, "X-Kinetica-Group"), None, "the default mode is not sent");
    }

    #[test]
    fn credentials_are_read_from_a_basic_header() {
        let headers = |value: &str| HashMap::from([("Authorization".to_string(), value.to_string())]);
        let read = |value: &str| credentials_from_http_headers(&headers(value), "u", "p");

        assert_eq!(read("Basic dXNlcjpwYXNz").unwrap(), ("user".into(), "pass".into()));
        // "a:b:c" keeps only "b" as the password
        assert_eq!(read("Basic YTpiOmM=").unwrap(), ("a".into(), "b".into()));
        // A bearer token, or a header that is not two words, leaves the options'
        assert_eq!(read("Bearer token").unwrap(), ("u".into(), "p".into()));
        assert_eq!(read("Basic a b").unwrap(), ("u".into(), "p".into()));
        // "user:" splits to one field, so reading the password fails
        assert_eq!(read("Basic dXNlcjo=").unwrap_err().to_string(), "Configuration error: The Basic authorization header's credentials have no ':' between user name and password");
        assert_eq!(credentials_from_http_headers(&HashMap::new(), "u", "p").unwrap(), ("u".into(), "p".into()));
    }

    #[test]
    fn sync_modes_are_named_by_their_wire_values() {
        assert_eq!(HaSynchronicityMode::from_value("repl_async"), Some(HaSynchronicityMode::Asynchronous));
        assert_eq!(HaSynchronicityMode::from_value("sync"), Some(HaSynchronicityMode::Synchronous));
        assert_eq!(HaSynchronicityMode::from_value("Synchronous_Parallel"), Some(HaSynchronicityMode::SynchronousParallel));
        assert_eq!(HaSynchronicityMode::from_value("default"), Some(HaSynchronicityMode::Default));
        assert_eq!(HaSynchronicityMode::from_value("nope"), None);
        // The options' header takes the wire value as well as the constant name
        assert_eq!(HaSynchronicityMode::from_header("SYNCHRONOUS").unwrap(), HaSynchronicityMode::Synchronous);
        assert_eq!(HaSynchronicityMode::from_header("REPL_SYNC").unwrap(), HaSynchronicityMode::Synchronous);
        assert_eq!(HaSynchronicityMode::from_header("NONE").unwrap(), HaSynchronicityMode::None);
        assert_eq!(
            HaSynchronicityMode::from_header("REPL_NOPE").unwrap_err().to_string(),
            "Configuration error: Unknown HA synchronicity mode: REPL_NOPE"
        );
        assert_eq!(HaSynchronicityMode::ALL[1].mode(), "REPL_NONE");
    }

    #[test]
    fn the_user_agent_names_the_client_and_version() {
        assert_eq!(sanitize_user_agent_token("My App/1.0"), "My_App_1.0");
        assert_eq!(sanitize_user_agent_token(""), "unknown");
        let user_agent = build_user_agent(Some("app"), None);
        let expected = format!("app {USER_AGENT_PROGRAM_NAME}/{} (Rust/", env!("GPUDB_API_VERSION"));
        assert!(user_agent.starts_with(&expected), "{user_agent}");
        assert!(user_agent.ends_with(')'));
        assert!(build_user_agent(Some(""), Some("1")).starts_with(USER_AGENT_PROGRAM_NAME), "an empty name is no name");
    }
}

/// Connecting, against stand-in servers: address resolution at construction,
/// the hostname regex fallback, and the multi-head probe.
#[cfg(test)]
mod connection_tests {
    use super::fake_server::{self, address, cluster_properties};
    use super::*;

    fn options() -> KineticaOptions {
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0);
        options
    }

    // The scheme, host, port and path, with one slash between path and
    // endpoint.
    #[test]
    fn an_endpoint_is_appended_with_one_slash() {
        let append = |url: &str, path: &str| append_path_to_url(&Url::parse(url).unwrap(), path).to_string();
        assert_eq!(append("http://h:9191", "/show/table"), "http://h:9191/show/table");
        assert_eq!(append("http://h:8082/gpudb-0", "/show/table"), "http://h:8082/gpudb-0/show/table");
        assert_eq!(append("http://h:8082/gpudb-0/", "/show/table"), "http://h:8082/gpudb-0/show/table");
        assert_eq!(append("http://u:p@h:9191/x?q=1#f", "/show/table"), "http://h:9191/x/show/table");
        assert_eq!(append("http://h:9191/?q=1", "/get/records/json?a=1&b=2"), "http://h:9191/get/records/json?a=1&b=2");
    }

    #[test]
    fn a_url_needs_http_or_https() {
        assert!(parse_absolute_url("http://h:9191").is_ok());
        assert!(parse_absolute_url("HTTPS://h").is_ok());
        for refused in ["ftp", "file", "jar", "mailto", "jrt", "myhost"] {
            assert_eq!(
                parse_absolute_url(&format!("{refused}:x")).unwrap_err(),
                format!("unsupported URL scheme '{refused}'; use http or https")
            );
        }
        assert_eq!(parse_absolute_url("myhost").unwrap_err(), "'myhost' has no URL scheme");
    }

    // A primary URL that cannot be read as one, such as host:port, is taken
    // for a host name, not added to the clusters to connect to.
    #[tokio::test]
    async fn a_primary_given_as_host_and_port_adds_no_cluster() {
        let server = fake_server::start("127.0.0.218").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let mut options = options();
        options.set_primary_url(format!("localhost:{}", server.url.port().unwrap()));
        let k = Kinetica::new(vec![server.url.to_string()], options).await.expect("client");
        assert_eq!(k.ha_ring_size(), 1);

        let err = Kinetica::new(vec!["localhost:9191".to_string()], self::options()).await.err().expect("refused");
        assert_eq!(err.to_string(), "API error: unsupported URL scheme 'localhost'; use http or https");
    }

    // The cluster in use must answer exactly "Kinetica is running!"; another
    // answer fails without failing over, and no answer fails over to the next
    // cluster.
    #[tokio::test]
    async fn a_ping_checks_the_answer_and_fails_over_when_there_is_none() {
        let answering = |server: &fake_server::FakeServer, answer: &'static str| {
            *server.behavior.json_handler.lock() =
                Some(Arc::new(move |path: &str, _: &[u8]| (path == "/").then(|| ("200 OK".to_string(), answer.to_string()))));
        };
        let a = fake_server::start("127.0.0.219").await;
        let b = fake_server::start("127.0.0.220").await;
        *a.behavior.properties.lock() = cluster_properties(&address(&a), &[]);
        *b.behavior.properties.lock() = cluster_properties(&address(&b), &[]);
        let k = Kinetica::new(vec![a.url.to_string(), b.url.to_string()], options()).await.expect("client");

        answering(&a, "Kinetica is running!");
        k.ping_with_failover().await.unwrap();

        answering(&a, "Kinetica is starting");
        assert_eq!(
            k.ping_with_failover().await.unwrap_err().to_string(),
            "API error: Server returned invalid response: Kinetica is starting"
        );
        assert_eq!(k.url(), Some(a.url.clone()), "no failover for an answer");

        // A answers 404 with nothing, as a server that is not Kinetica's
        *a.behavior.json_handler.lock() = None;
        answering(&b, "Kinetica is running!");
        k.ping_with_failover().await.unwrap();
        assert_eq!(k.url(), Some(b.url.clone()), "failed over to B");

        // Neither answers: the last attempt's failure, as a connection error
        *b.behavior.json_handler.lock() = None;
        let err = k.ping_with_failover().await.unwrap_err();
        assert_eq!(err.to_string(), "Connection error: Server returned HTTP 404 (Not Found).");
    }

    // Invalid option values are refused when the options are used, before
    // anything is sent.
    #[tokio::test]
    async fn invalid_options_are_refused_before_anything_is_sent() {
        let refused = |change: fn(&mut KineticaOptions)| async move {
            let mut options = options();
            change(&mut options);
            Kinetica::new(vec!["http://127.0.0.1:1".to_string()], options).await.err().expect("refused").to_string()
        };
        assert_eq!(
            refused(|o| { o.set_max_total_connections(0); }).await,
            "Configuration error: max_total_connections must be greater than zero."
        );
        assert_eq!(
            refused(|o| { o.set_max_connections_per_host(0); }).await,
            "Configuration error: max_connections_per_host must be greater than zero."
        );
        assert_eq!(
            refused(|o| { o.set_initial_connection_attempt_timeout(9_223_372_036_855); }).await,
            "Configuration error: initial_connection_attempt_timeout must be at most 9223372036854 ms; given 9223372036855"
        );
    }

    // A URL string may be a comma-separated list, and an empty list of URLs
    // is refused.
    #[tokio::test]
    async fn a_url_string_may_list_several_clusters() {
        let first = fake_server::start("127.0.0.96").await;
        let second = fake_server::start("127.0.0.97").await;
        *first.behavior.properties.lock() = cluster_properties(&address(&first), &[]);
        *second.behavior.properties.lock() = cluster_properties(&address(&second), &[]);

        let k = Kinetica::new(vec![format!("{},{}", address(&first), address(&second))], options())
            .await
            .expect("client");
        assert_eq!(k.ha_ring_size(), 2);

        let err = Kinetica::connect(Vec::new(), options()).await.err().expect("refused");
        assert_eq!(err.to_string(), "API error: Must provide at least one URL; gave none!");
    }

    #[tokio::test]
    async fn a_client_reads_the_server_version_and_records_its_only_cluster_as_primary() {
        let head = fake_server::start("127.0.0.11").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[]);

        let k = Kinetica::new(vec![head.url.to_string()], options()).await.expect("client");

        assert_eq!(k.server_version(), Some(&KineticaVersion::with_build(7, 2, 3, 27, 20261002000000)));
        assert_eq!(k.ha_ring_size(), 1);
        assert_eq!(k.options.primary_url, head.url.to_string(), "the primary is saved in the options");
        assert!(k.ha_ring_info()[0].is_primary_cluster());
    }

    // The head rank answers at a later address than its first, so
    // multi-head stays on; the connection stays on the given URL even so,
    // since what answered elsewhere may be another Kinetica.
    #[tokio::test]
    async fn the_given_url_stays_the_head_when_a_later_head_address_answers() {
        let given = fake_server::start("127.0.0.211").await;
        let other = fake_server::start("127.0.0.212").await;
        let mut properties = cluster_properties("http://127.0.0.13:1", &[]);
        properties.insert("conf.worker_http_server_urls_private".to_string(), address(&other));
        *given.behavior.properties.lock() = properties;

        let k = Kinetica::new(vec![given.url.to_string()], options()).await.expect("client");

        assert_eq!(other.behavior.count("/show/system/status"), 1, "the later address was probed and answered");
        assert_eq!(k.url(), Some(given.url.clone()), "the given URL is kept");
        let hm = k.hm_url().unwrap();
        assert_eq!((hm.host_str(), hm.port()), (Some("127.0.0.211"), Some(DEFAULT_HOST_MANAGER_PORT)));
        assert!(k.ha_ring_info()[0].is_multi_head_available());
        assert_eq!(k.options.primary_url, given.url.to_string());
    }

    /// Two instances on one machine: the head address the cluster advertises
    /// answers, but reports another cluster, so it is passed over and the
    /// connection is head-node-only on the given URL.
    #[tokio::test]
    async fn a_head_address_reporting_another_cluster_is_passed_over() {
        let given = fake_server::start("127.0.0.213").await;
        let other = fake_server::start("127.0.0.214").await;
        let mut properties = cluster_properties(&address(&other), &[]);
        properties.insert("conf.ring_name".to_string(), "ring-a".to_string());
        *given.behavior.properties.lock() = properties;
        let mut other_properties = cluster_properties(&address(&other), &[]);
        other_properties.insert("conf.ring_name".to_string(), "ring-b".to_string());
        *other.behavior.properties.lock() = other_properties;

        let k = Kinetica::new(vec![given.url.to_string()], options()).await.expect("client");

        assert_eq!(other.behavior.count("/show/system/properties"), 1, "its identity was read");
        assert_eq!(k.url(), Some(given.url.clone()));
        assert!(!k.ha_ring_info()[0].is_multi_head_available());
    }

    /// A worker address that answers but reports another cluster does not
    /// count as the worker answering, and costs multi-head.
    #[tokio::test]
    async fn a_worker_address_reporting_another_cluster_costs_multi_head() {
        let head = fake_server::start("127.0.0.215").await;
        let other = fake_server::start("127.0.0.216").await;
        let mut properties = cluster_properties(&address(&head), &[&address(&other)]);
        properties.insert("conf.ring_name".to_string(), "ring-a".to_string());
        *head.behavior.properties.lock() = properties;
        let mut other_properties = cluster_properties(&address(&other), &[]);
        other_properties.insert("conf.ring_name".to_string(), "ring-b".to_string());
        *other.behavior.properties.lock() = other_properties;

        let k = Kinetica::new(vec![head.url.to_string()], options()).await.expect("client");
        assert!(!k.probe_current_cluster_multi_head().await);

        // The same worker reporting the head's cluster answers
        *other.behavior.properties.lock() = head.behavior.properties.lock().clone();
        assert!(k.probe_current_cluster_multi_head().await);
    }

    /// The host manager address an operator published for the head's host is
    /// used as it is; failing that, the server's port, unless the caller chose
    /// one.
    #[tokio::test]
    async fn the_host_manager_url_is_the_published_one_or_on_the_reported_port() {
        let head = fake_server::start("127.0.0.217").await;
        let mut properties = cluster_properties(&address(&head), &[]);
        properties.insert("conf.host0_host_manager_public_url".to_string(), "http://127.0.0.217:9400/hm".to_string());
        *head.behavior.properties.lock() = properties.clone();
        let k = Kinetica::new(vec![head.url.to_string()], options()).await.expect("client");
        assert_eq!(k.hm_url(), Some(Url::parse("http://127.0.0.217:9400/hm").unwrap()));

        properties.remove("conf.host0_host_manager_public_url");
        properties.insert("conf.hm_http_port".to_string(), "9555".to_string());
        *head.behavior.properties.lock() = properties;
        let k = Kinetica::new(vec![head.url.to_string()], options()).await.expect("client");
        assert_eq!(k.hm_url().unwrap().port(), Some(9555), "the server's port, the caller's being the default");

        let mut chosen = options();
        chosen.set_host_manager_port(9301);
        let k = Kinetica::new(vec![head.url.to_string()], chosen).await.expect("client");
        assert_eq!(k.hm_url().unwrap().port(), Some(9301), "the caller's port, chosen");
    }

    #[tokio::test]
    async fn a_regex_matching_only_the_given_url_falls_back_to_it_head_node_only() {
        let given = fake_server::start("127.0.0.14").await;
        *given.behavior.properties.lock() = cluster_properties("http://10.9.9.9:9191", &[]);

        let mut options = options();
        options.set_hostname_regex(Some(r"127\.0\.0\.".to_string()));
        let k = Kinetica::new(vec![given.url.to_string()], options).await.expect("client");

        assert!(!k.is_auto_discovery_enabled(), "used directly, without discovery");
        assert_eq!(k.url(), Some(given.url.clone()));
    }

    #[tokio::test]
    async fn a_regex_matching_nothing_at_all_fails_the_connection() {
        let given = fake_server::start("127.0.0.15").await;
        *given.behavior.properties.lock() = cluster_properties("http://10.9.9.9:9191", &[]);

        let mut options = options();
        options.set_hostname_regex(Some(r"192\.168\.".to_string()));
        let err = Kinetica::new(vec![given.url.to_string()], options).await.err().expect("refused");

        assert_eq!(
            err.to_string(),
            format!(
                "API error: Could not connect to any working Kinetica server due to hostname regex mismatch \
                 (given URLs: [{}]): the pattern <192\\.168\\.> matches no address the server advertises and \
                 none of the given URLs either; No valid matching IP/hostname found for the head rank",
                given.url
            )
        );
    }

    #[tokio::test]
    async fn the_probe_finds_each_worker_at_an_address_that_answers() {
        let head = fake_server::start("127.0.0.16").await;
        let worker = fake_server::start("127.0.0.17").await;
        let mut properties = cluster_properties(&address(&head), &["http://127.0.0.18:1"]);
        properties.insert(
            "conf.worker_http_server_urls_private".to_string(),
            format!("{};{}", address(&head), address(&worker)),
        );
        *head.behavior.properties.lock() = properties;

        let k = Kinetica::new(vec![head.url.to_string()], options()).await.expect("client");
        assert!(k.probe_current_cluster_multi_head().await, "every worker answered somewhere");

        let cluster = k.cluster_info().unwrap();
        assert_eq!(cluster.worker_rank_urls(), [Some(worker.url.clone())]);
    }

    // A rank is probed by a quick check: one that takes the connection and
    // never answers is given up on within the fast client's limits, not the
    // connection's timeout, which here is none.
    #[tokio::test]
    async fn a_hung_worker_is_given_up_on_by_the_quick_check() {
        let head = fake_server::start("127.0.0.231").await;
        let hung = tokio::net::TcpListener::bind("127.0.0.232:0").await.unwrap();
        let hung_url = format!("http://{}", hung.local_addr().unwrap());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = hung.accept().await {
                held.push(stream); // read nothing, answer nothing
            }
        });
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[&hung_url]);

        let k = Kinetica::new(vec![head.url.to_string()], options()).await.expect("client");
        assert_eq!(k.options.timeout, 0, "no request timeout");
        let probed = tokio::time::timeout(std::time::Duration::from_secs(10), k.probe_current_cluster_multi_head())
            .await
            .expect("the probe gave up on the hung worker");
        assert!(!probed);
    }

    #[tokio::test]
    async fn a_worker_answering_nowhere_keeps_its_first_address_and_costs_multi_head() {
        let head = fake_server::start("127.0.0.19").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &["http://127.0.0.20:1,http://127.0.0.21:1"]);

        let k = Kinetica::new(vec![head.url.to_string()], options()).await.expect("client");
        assert!(!k.probe_current_cluster_multi_head().await);

        let cluster = k.cluster_info().unwrap();
        assert_eq!(cluster.worker_rank_urls(), [Some(Url::parse("http://127.0.0.20:1").unwrap())]);
        assert!(!cluster.is_multi_head_available());
    }

    // Only a 401 or 403 is an authorization failure: a status check failing
    // with a server error that speaks of authentication means only that the
    // system is not known to be running.
    #[tokio::test]
    async fn a_server_error_about_authentication_is_not_unauthorized() {
        let server = fake_server::start("127.0.0.233").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let k = Kinetica::new(vec![server.url.to_string()], options()).await.expect("client");

        *server.behavior.handler.lock() = Some(Arc::new(|path: &str, _: &[u8]| {
            path.ends_with("/show/system/status").then(|| Err("Authentication token expired (unauthorized)".to_string()))
        }));
        assert_eq!(k.get_system_running_status(&server.url, false).await.unwrap(), (false, None));
    }

    #[tokio::test]
    async fn a_system_is_running_only_when_its_status_says_exactly_running() {
        let server = fake_server::start("127.0.0.22").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let k = Kinetica::new(vec![server.url.to_string()], options()).await.expect("client");

        let status = |pairs: &[(&str, &str)]| {
            pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect::<HashMap<_, _>>()
        };
        for (map, expected) in [
            (status(&[("system", r#"{"status":"running"}"#)]), (true, None)),
            (status(&[("system", r#"{"status":"RUNNING"}"#)]), (false, None)),
            (status(&[("status", "running")]), (false, None)),
            (status(&[("system", "not json")]), (false, None)),
            (
                status(&[("system", r#"{"status":"running"}"#), ("ha_status", r#"{"drained":"draining"}"#)]),
                (true, Some("draining".to_string())),
            ),
            (status(&[("system", r#"{"status":"running"}"#), ("ha_status", "not json")]), (false, None)),
        ] {
            *server.behavior.status.lock() = Some(map.clone());
            assert_eq!(k.get_system_running_status(&server.url, false).await.unwrap(), expected, "{map:?}");
        }

        // Running, but draining its HA queue, is not usable
        *server.behavior.status.lock() =
            Some(status(&[("system", r#"{"status":"running"}"#), ("ha_status", r#"{"drained":"draining"}"#)]));
        assert!(!k.is_cluster_usable(&server.url, true).await);
    }

    #[tokio::test]
    async fn a_cluster_is_probed_once_per_arrival_and_again_only_when_its_topology_may_have_moved() {
        let head = fake_server::start("127.0.0.25").await;
        let worker = fake_server::start("127.0.0.26").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[&address(&worker)]);
        let k = Kinetica::new(vec![head.url.to_string()], options()).await.expect("client");
        let properties_reads = || head.behavior.count("/show/system/properties");

        // The arrival at construction owes a probe; the first ask performs it
        assert!(k.cluster_info().unwrap().is_probe_owed());
        let before = properties_reads();
        let first = k.acquire_multi_head_snapshot(false).await.unwrap();
        assert_eq!(properties_reads(), before + 1);
        assert!(!k.cluster_info().unwrap().is_probe_owed());
        assert!(first.is_multi_head_available());
        assert_eq!(first.worker_rank_urls(), [Some(worker.url.clone())]);

        // Asking again is answered from what the connection holds
        let second = k.acquire_multi_head_snapshot(false).await.unwrap();
        assert_eq!(properties_reads(), before + 1, "no second probe for the same arrival");
        assert!(Arc::ptr_eq(&first, &second), "the same answer");

        // A possible topology change re-probes, and publishes a new answer
        let third = k.acquire_multi_head_snapshot(true).await.unwrap();
        assert_eq!(properties_reads(), before + 2);
        assert!(!Arc::ptr_eq(&second, &third), "a re-probe is a change, even with the same addresses");
        assert_eq!(third.worker_rank_urls(), second.worker_rank_urls());
    }

    #[test]
    fn a_bare_cluster_behind_httpd_reaches_its_host_manager_on_the_head_nodes_port() {
        let proxied = ClusterAddressInfo::with_host_manager_port(Url::parse("https://h:8082/gpudb-0").unwrap(), 9300).unwrap();
        assert_eq!(proxied.host_manager_url().as_str(), "https://h:8082/gpudb-host-manager");
        let direct = ClusterAddressInfo::with_host_manager_port(Url::parse("http://h:9191").unwrap(), 9300).unwrap();
        assert_eq!(direct.host_manager_url().as_str(), "http://h:9300/");
    }

    // A URL whose path is a lone slash has no path to rewrite, so its host
    // manager is on the host manager port, behind httpd or not.
    #[tokio::test]
    async fn a_trailing_slash_is_no_path_to_the_host_manager() {
        let bare = ClusterAddressInfo::with_host_manager_port(Url::parse("http://h:9191/").unwrap(), 9300).unwrap();
        assert_eq!(bare.host_manager_url().as_str(), "http://h:9300/");

        let server = fake_server::start("127.0.0.25").await;
        let mut properties = cluster_properties(&address(&server), &[]);
        properties.insert("conf.enable_httpd_proxy".to_string(), "TRUE".to_string());
        *server.behavior.properties.lock() = properties;
        let k = Kinetica::new(vec![format!("{}/", address(&server))], options()).await.expect("client");
        let host_manager_url = k.hm_url().unwrap();
        assert_eq!((host_manager_url.port(), host_manager_url.path()), (Some(9300), "/"));
    }
}

/// Each connection is a server session of its own: its ID goes with every
/// request, and the session is dropped on close, or failing that when the
/// connection is dropped, once the server has reported it used.
#[cfg(test)]
mod session_tests {
    use super::fake_server::{self, address, cluster_properties, encode};
    use super::*;
    use crate::{DropSessionRequest, DropSessionResponse, HasTableResponse};

    fn options() -> KineticaOptions {
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_username("u").set_password("p");
        options
    }

    /// The value of `name` in a request head, if it was sent.
    fn header(head: &str, name: &str) -> Option<String> {
        head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim().to_string())
        })
    }

    /// A server answering `/has/table` with `info`, and `/drop/session` with
    /// `drop_reply`, recording the session ID of each drop it is sent.
    fn serve(server: &fake_server::FakeServer, info: &'static [(&'static str, &'static str)], drop_reply: Result<(), &'static str>) -> Arc<parking_lot::Mutex<Vec<String>>> {
        let drops: Arc<parking_lot::Mutex<Vec<String>>> = Default::default();
        let log = drops.clone();
        // Weak, as the behavior holds this handler
        let behavior = Arc::downgrade(&server.behavior);
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| match path {
            "/has/table" => Some(Ok(encode(
                &crate::generated::HAS_TABLE_RESPONSE_SCHEMA,
                &HasTableResponse {
                    table_name: "t".into(),
                    table_exists: true,
                    info: info.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect(),
                },
            ))),
            "/drop/session" => {
                let _: DropSessionRequest =
                    serde_avro_fast::from_datum_slice(body, &crate::generated::DROP_SESSION_REQUEST_SCHEMA).unwrap();
                // The session to drop is named by the request's header
                let behavior = behavior.upgrade().expect("the server is running");
                let session_id = header(&behavior.heads.lock()["/drop/session"], "X-Kinetica-Session").unwrap_or_default();
                log.lock().push(session_id.clone());
                Some(drop_reply.map_err(str::to_string).map(|()| {
                    encode(
                        &crate::generated::DROP_SESSION_RESPONSE_SCHEMA,
                        &DropSessionResponse { session_id, info: HashMap::new() },
                    )
                }))
            }
            _ => None,
        }));
        drops
    }

    async fn connect(server: &fake_server::FakeServer) -> Kinetica {
        *server.behavior.properties.lock() = cluster_properties(&address(server), &[]);
        Kinetica::new(vec![server.url.to_string()], options()).await.expect("client")
    }

    #[tokio::test]
    async fn every_request_carries_the_connections_own_session_id() {
        let server = fake_server::start("127.0.0.221").await;
        serve(&server, &[], Ok(()));
        let k = connect(&server).await;
        let other = connect(&server).await;

        let head = server.behavior.heads.lock().get("/show/system/properties").cloned().unwrap();
        let id = header(&head, "X-Kinetica-Session").expect("sent");
        assert_eq!(id, other.session.id, "the last connection's");
        assert_ne!(k.session.id, other.session.id, "one session per connection");
        assert!(uuid::Uuid::parse_str(&k.session.id).is_ok());

        // After the caller's headers, and before the group and authorization
        let position = |name: &str| head.find(&format!("\n{name}:")).or_else(|| head.find(&format!("\n{}:", name.to_lowercase())));
        let session = position("X-Kinetica-Session").unwrap();
        assert!(position("User-Agent").unwrap() < session && session < position("Authorization").unwrap(), "{head}");

        // On every request, the ping included
        k.has_table("t".into()).await.unwrap();
        let head = server.behavior.heads.lock().get("/has/table").cloned().unwrap();
        assert_eq!(header(&head, "X-Kinetica-Session").as_deref(), Some(k.session.id.as_str()));
        k.ping(&server.url).await;
        let head = server.behavior.heads.lock().get("/").cloned().unwrap();
        assert_eq!(header(&head, "X-Kinetica-Session").as_deref(), Some(k.session.id.as_str()));
    }

    #[tokio::test]
    async fn the_session_header_is_the_clients_own() {
        let server = fake_server::start("127.0.0.222").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let mut options = options();
        options.add_http_header("X-Kinetica-Session", "mine");
        let k = Kinetica::new(vec![server.url.to_string()], options).await.expect("client");

        let head = server.behavior.heads.lock().get("/show/system/properties").cloned().unwrap();
        assert_eq!(header(&head, "X-Kinetica-Session").as_deref(), Some(k.session.id.as_str()));
        assert_eq!(
            k.add_http_header("X-Kinetica-Session", "x").unwrap_err().to_string(),
            "API error: Not allowed to change protected header: X-Kinetica-Session"
        );
        assert!(k.remove_http_header("X-Kinetica-Session").is_err());
    }

    /// The server reporting the session used arms the drop; closing sends
    /// it, once, and leaves the connection refusing requests.
    #[tokio::test]
    async fn a_session_reported_used_is_dropped_on_close() {
        let server = fake_server::start("127.0.0.223").await;
        let drops = serve(&server, &[("session_id_used", "true")], Ok(()));
        let k = connect(&server).await;

        k.has_table("t".into()).await.unwrap();
        assert!(k.session.is_armed());
        k.close().await;
        k.close().await;

        assert_eq!(*drops.lock(), std::slice::from_ref(&k.session.id), "dropped once");
        let head = server.behavior.heads.lock().get("/drop/session").cloned().unwrap();
        assert_eq!(header(&head, "X-Kinetica-Session").as_deref(), Some(k.session.id.as_str()));
        assert!(k.is_closed());

        let err = k.has_table("t".into()).await.unwrap_err();
        assert_eq!(
            err.message(),
            format!("Cannot submit a request to {}has/table: the connection has been closed", server.url)
        );
        assert_eq!(server.behavior.count("/has/table"), 1, "nothing sent after closing");
    }

    #[tokio::test]
    async fn a_session_never_reported_used_is_not_dropped() {
        for info in [&[][..], &[("session_id_used", "false")][..], &[("session_id_used", "TRUE")][..]] {
            let server = fake_server::start("127.0.0.224").await;
            let drops = serve(&server, info, Ok(()));
            let k = connect(&server).await;

            k.has_table("t".into()).await.unwrap();
            k.close().await;
            assert!(drops.lock().is_empty(), "{info:?}");
        }
    }

    /// A drop the server refuses, as one without session support does, is
    /// logged; the connection closes all the same.
    #[tokio::test]
    async fn a_failed_drop_still_closes_the_connection() {
        let server = fake_server::start("127.0.0.225").await;
        let drops = serve(&server, &[("session_id_used", "true")], Err("Unknown URI: /drop/session"));
        let k = connect(&server).await;

        k.has_table("t".into()).await.unwrap();
        k.close().await;
        assert_eq!(drops.lock().len(), 1);
        assert!(k.has_table("t".into()).await.is_err());
    }

    async fn connect_to_session(server: &fake_server::FakeServer, session_id: &str) -> Kinetica {
        *server.behavior.properties.lock() = cluster_properties(&address(server), &[]);
        let mut options = options();
        options.set_session_id(Some(session_id));
        Kinetica::new(vec![server.url.to_string()], options).await.expect("client")
    }

    /// A session the caller names is shared by the connections given it, and
    /// is the caller's to drop: neither closing nor dropping a connection
    /// drops it, even once it is reported used.
    #[tokio::test]
    async fn a_callers_session_is_shared_and_never_dropped_for_it() {
        let server = fake_server::start("127.0.0.228").await;
        let drops = serve(&server, &[("session_id_used", "true")], Ok(()));
        let k = connect_to_session(&server, "shared").await;
        let other = connect_to_session(&server, "shared").await;

        for connection in [&k, &other] {
            connection.has_table("t".into()).await.unwrap();
            let head = server.behavior.heads.lock().get("/has/table").cloned().unwrap();
            assert_eq!(header(&head, "X-Kinetica-Session").as_deref(), Some("shared"));
            assert!(!connection.session.is_armed());
        }

        k.close().await;
        drop(other);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(drops.lock().is_empty(), "{:?}", drops.lock());
    }

    /// The caller drops a session it named with `drop_session`, on any
    /// connection given it.
    #[tokio::test]
    async fn the_caller_drops_its_session_by_name() {
        let server = fake_server::start("127.0.0.229").await;
        let drops = serve(&server, &[], Ok(()));
        let k = connect_to_session(&server, "shared").await;

        let response = k.drop_session().await.unwrap();
        assert_eq!(response.session_id, "shared");
        assert_eq!(*drops.lock(), ["shared"]);
    }

    /// An empty session ID names no session: the connection has its own,
    /// which it drops.
    #[tokio::test]
    async fn an_empty_session_id_gives_a_session_of_the_connections_own() {
        let server = fake_server::start("127.0.0.230").await;
        let drops = serve(&server, &[("session_id_used", "true")], Ok(()));
        let k = connect_to_session(&server, "").await;

        assert!(uuid::Uuid::parse_str(&k.session.id).is_ok());
        k.has_table("t".into()).await.unwrap();
        k.close().await;
        assert_eq!(*drops.lock(), std::slice::from_ref(&k.session.id));
    }

    /// Dropped without being closed, a connection still drops its session,
    /// with the request prepared when the session was reported used.
    #[tokio::test]
    async fn a_connection_dropped_unclosed_drops_its_session() {
        let server = fake_server::start("127.0.0.226").await;
        let drops = serve(&server, &[("session_id_used", "true")], Ok(()));
        let k = connect(&server).await;
        k.has_table("t".into()).await.unwrap();
        let id = k.session.id.clone();

        drop(k);
        for _ in 0..100 {
            if !drops.lock().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(*drops.lock(), std::slice::from_ref(&id));
        let head = server.behavior.heads.lock().get("/drop/session").cloned().unwrap();
        assert_eq!(header(&head, "X-Kinetica-Session").as_deref(), Some(id.as_str()));
        assert_eq!(header(&head, "Content-type").as_deref(), Some("application/octet-stream"));

        // Never armed, nothing is sent
        let quiet = fake_server::start("127.0.0.227").await;
        let quiet_drops = serve(&quiet, &[], Ok(()));
        drop(connect(&quiet).await);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(quiet_drops.lock().is_empty());
    }

    /// Only the response's `info` map counts, and only "true".
    #[test]
    fn only_the_info_map_reports_a_session_used() {
        let schema = FastSchema::from_str(
            r#"{"type":"record","name":"r","fields":[{"name":"data","type":"string"},{"name":"info","type":{"type":"map","values":"string"}}]}"#,
        )
        .unwrap();
        #[derive(serde::Serialize)]
        struct R {
            data: String,
            info: HashMap<String, String>,
        }
        let payload = |data: &str, info: &[(&str, &str)]| {
            encode(&schema, &R { data: data.to_string(), info: info.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect() })
        };

        assert!(reports_session_used(&payload("", &[("session_id_used", "true")]), &schema));
        assert!(!reports_session_used(&payload("session_id_used", &[]), &schema), "in the data, not the info");
        assert!(!reports_session_used(&payload("", &[("session_id_used", "yes")]), &schema));
        assert!(!reports_session_used(&payload("", &[]), &schema));

        // A response with no info map reports nothing
        let bare = FastSchema::from_str(r#"{"type":"record","name":"r","fields":[{"name":"data","type":"string"}]}"#).unwrap();
        #[derive(serde::Serialize)]
        struct Bare {
            data: String,
        }
        assert!(!reports_session_used(&encode(&bare, &Bare { data: "session_id_used".into() }), &bare));
    }
}
