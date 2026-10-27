//! Error types for the Kinetica client.
//!
//! All errors are wrapped in the `KineticaError` enum which implements
//! `std::error::Error` for easy integration with other Rust error handling.

use thiserror::Error;

/// The main error type for all Kinetica operations.
///
/// All API calls return `Result<T, KineticaError>` to handle various failure modes.
///
/// The variant tells what kind of failure it was: a request that failed on
/// every cluster it could reach is a [`KineticaError::Connection`]. The URL a
/// request failed at is in its message.
///
/// # Examples
///
/// ```
/// use gpudb_api::KineticaError;
///
/// fn example() -> Result<(), KineticaError> {
///     // Operations that might fail...
///     Ok(())
/// }
/// ```
#[derive(Error, Debug, Clone)]
pub enum KineticaError {
    /// HTTP/network communication error
    #[error("HTTP error: {0}")]
    Http(String),

    /// Avro serialization/deserialization error
    #[error("Avro error: {0}")]
    Avro(String),

    /// Server-side API error (returned by Kinetica)
    #[error("API error: {0}")]
    Api(String),

    /// Unauthorized access error (authentication failed)
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Hostname regex mismatch error
    #[error("Hostname regex failure: {0}")]
    HostnameRegexFailure(String),

    /// Connection error (server not reachable or not running)
    #[error("Connection error: {0}")]
    Connection(String),

    /// Configuration error (invalid parameters, missing permissions, etc.)
    #[error("Configuration error: {0}")]
    Config(String),

    /// HA failover is disabled
    #[error("Failover disabled: {0}")]
    FailoverDisabled(String),

    /// HA failover unavailable (all clusters tried)
    #[error("HA unavailable: {0}")]
    HaUnavailable(String),

    /// Server exit/shutdown in progress
    #[error("Server exit: {0}")]
    Exit(String),

    /// Internal client error
    #[error("Internal error")]
    Internal,

    /// An error reported in terms of the one that caused it.
    ///
    /// Recovery paths use this to report what actually went wrong while keeping the
    /// underlying failure reachable through
    /// [`std::error::Error::source`] — a retry's own failure is usually a
    /// consequence of the original, and replacing it loses the only useful
    /// message.
    #[error("{message}")]
    Caused {
        message: String,
        #[source]
        source: Box<KineticaError>,
    },
}

impl KineticaError {
    /// Reports `message`, keeping `source` as the underlying cause.
    ///
    /// See [`KineticaError::Caused`].
    pub fn caused(message: impl Into<String>, source: KineticaError) -> Self {
        Self::Caused {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// The error's message without the kind of error before it.
    pub fn message(&self) -> String {
        match self {
            KineticaError::Http(m)
            | KineticaError::Avro(m)
            | KineticaError::Api(m)
            | KineticaError::Unauthorized(m)
            | KineticaError::HostnameRegexFailure(m)
            | KineticaError::Connection(m)
            | KineticaError::Config(m)
            | KineticaError::FailoverDisabled(m)
            | KineticaError::HaUnavailable(m)
            | KineticaError::Exit(m) => m.clone(),
            KineticaError::Internal => self.to_string(),
            KineticaError::Caused { message, .. } => message.clone(),
        }
    }

    /// The error that caused this one, as its `Display` describes it: the
    /// root of the chain, with its kind, such as "Server exit: Kinetica is
    /// exiting". Messages that report a failure by its cause use it.
    pub(crate) fn cause_description(&self) -> String {
        self.root_cause().to_string()
    }

    /// The error underneath this one, following the chain to its root.
    ///
    /// Returns `self` when nothing is attached. Useful for inspecting what
    /// actually failed when an error has been re-reported by a recovery path.
    pub fn root_cause(&self) -> &KineticaError {
        let mut current = self;
        while let KineticaError::Caused { source, .. } = current {
            current = source;
        }
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    // A recovery path reports what went wrong while keeping the underlying
    // failure reachable.
    #[test]
    fn a_cause_stays_reachable_through_the_error_trait() {
        let original = KineticaError::Config("no shard mapping".to_string());
        let reported = KineticaError::caused("lookup failed", original);

        assert_eq!(reported.to_string(), "lookup failed");
        assert_eq!(
            reported.source().map(ToString::to_string),
            Some("Configuration error: no shard mapping".to_string())
        );
    }

    #[test]
    fn root_cause_follows_the_whole_chain() {
        let root = KineticaError::Connection("worker unreachable".to_string());
        let once = KineticaError::caused("rebuild failed", root);
        let twice = KineticaError::caused("retry also failed", once);

        assert!(matches!(twice.root_cause(), KineticaError::Connection(_)));
        assert_eq!(
            twice.root_cause().to_string(),
            "Connection error: worker unreachable"
        );
    }

    #[test]
    fn an_error_with_no_cause_is_its_own_root() {
        let plain = KineticaError::Api("boom".to_string());
        assert_eq!(plain.root_cause().to_string(), plain.to_string());
        assert!(plain.source().is_none());
    }

    /// The original failure is the diagnosis. A retry's own failure is usually a
    /// consequence of it, so it is appended rather than substituted.
    #[test]
    fn a_retry_failure_is_appended_not_substituted() {
        let original = KineticaError::Config("unshardable key".to_string());
        let retry = KineticaError::Http("connection reset".to_string());

        let reported = KineticaError::caused(
            format!("{original}; the retry after recovery also failed: {retry}"),
            original,
        );

        let text = reported.to_string();
        assert!(text.contains("unshardable key"), "got: {text}");
        assert!(text.contains("connection reset"), "got: {text}");
        assert!(matches!(reported.root_cause(), KineticaError::Config(_)));
    }

    /// A failure is described by its cause, with its kind, as the failover
    /// message and the BulkInserter's give it.
    #[test]
    fn a_failure_is_described_by_its_cause() {
        assert_eq!(KineticaError::Connection("refused".into()).cause_description(), "Connection error: refused");
        assert_eq!(KineticaError::Http("cut".into()).cause_description(), "HTTP error: cut");
        assert_eq!(
            KineticaError::caused("wrapped", KineticaError::Exit("gone".into())).cause_description(),
            "Server exit: gone"
        );
        assert_eq!(
            crate::gpudb::failover_exhausted(&KineticaError::Exit("gone".into()), "Only one cluster in ring; HA failover unavailable")
                .to_string(),
            "Connection error: Server exit: gone; Only one cluster in ring; HA failover unavailable"
        );
    }
}
