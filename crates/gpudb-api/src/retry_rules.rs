//! When a failed request is sent again to the same server, when it fails
//! over to another cluster, and when its failure goes back to the caller.
//!
//! Every re-send of a request to one server is made by the HTTP layer
//! ([`crate::hyper_client::HyperClient`]) and counted against one budget:
//! the connection's `max_retries`, or a bulk-insert batch's own. No other
//! layer re-sends to the same server; the layers above fail over.
//!
//! A reply is decided by what the server said before its status: one status
//! carries different conditions, and a `503` can mean a server that is
//! offline, which a re-send cannot change, or one that is busy, which it may.

use crate::KineticaError;
use std::time::Duration;

/// Whether a failed attempt may be sent again to the same server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resend {
    /// Re-send it, up to the request's budget: nothing reached the server,
    /// the request was lost before it was written in full, or the server or
    /// a gateway could not take it at that moment.
    Always,
    /// Re-send it, up to the request's budget, after a wait that grows with
    /// each re-send; see [`resend_wait`]. The rank refused the request before
    /// handling it because it is rebuilding its HA client, which takes
    /// seconds: nothing was stored, and an immediate re-send would spend the
    /// budget on a condition it cannot outlast.
    AfterWait,
    /// Re-send it only if a second copy is harmless: the server may have
    /// applied it, or some of its ranks may have stored their part of it.
    IfRepeatable,
    /// Never re-send it to the same server.
    Never,
}

/// What the client does with a reply that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reply {
    /// Re-send it, then fail over.
    Resend,
    /// Re-send it after a wait, then fail over: the rank is rebuilding its HA
    /// client, and refused the request before handling it.
    RankNotReady,
    /// Fail over without re-sending.
    FailOver,
    /// Fail over, re-sending only if a second copy is harmless.
    RankFailure,
    /// Report it to the caller, without re-sending or failing over.
    Report,
}

impl Reply {
    /// Whether, and how, a reply of this kind may be re-sent to the same
    /// server; `None` for one reported to the caller.
    pub(crate) fn resend(self) -> Option<Resend> {
        match self {
            Reply::Resend => Some(Resend::Always),
            Reply::RankNotReady => Some(Resend::AfterWait),
            Reply::FailOver => Some(Resend::Never),
            Reply::RankFailure => Some(Resend::IfRepeatable),
            Reply::Report => None,
        }
    }
}

pub(crate) const DB_OFFLINE_ERROR_MESSAGE: &str = "Kinetica is offline";
pub(crate) const DB_EXITING_ERROR_MESSAGE: &str = "Kinetica is exiting";
pub(crate) const DB_SHUTTING_DOWN_ERROR_MESSAGE: &str = "System shutting down";
pub(crate) const DB_STARTING_UP_ERROR_MESSAGE: &str = "Kinetica is starting up";
pub(crate) const DB_RANK_DOWN_ERROR_MESSAGE: &str = "Kinetica is currently unable to process this message";
pub(crate) const DB_ADMIN_ONLY_ERROR_MESSAGE: &str = "Kinetica is restricted to admin users only";
pub(crate) const DB_SYSTEM_LIMITED_ERROR_MESSAGE: &str = "system-limited-fatal";
pub(crate) const DB_DRAINING_HAQ_ERROR_MESSAGE: &str = "Unavailable: Draining HA queue";
pub(crate) const DB_QP_DOWN_ERROR_MESSAGE: &str = "Query planner is not running";

/// Messages reporting a server state that a second copy of the request would
/// meet again.
const SERVER_STATE_MESSAGES: [&str; 9] = [
    DB_EXITING_ERROR_MESSAGE,
    DB_SHUTTING_DOWN_ERROR_MESSAGE,
    DB_OFFLINE_ERROR_MESSAGE,
    DB_STARTING_UP_ERROR_MESSAGE,
    DB_RANK_DOWN_ERROR_MESSAGE,
    DB_ADMIN_ONLY_ERROR_MESSAGE,
    DB_SYSTEM_LIMITED_ERROR_MESSAGE,
    DB_DRAINING_HAQ_ERROR_MESSAGE,
    DB_QP_DOWN_ERROR_MESSAGE,
];

/// The reason phrases, in lower case, of a `503` reporting a server state.
/// Two states, migrating and irrecoverable, are shown only here; the others
/// are also in the message, but the reason phrase is the one part of the
/// reply that every encoding shares.
const SERVER_STATE_REASONS: [&str; 8] = [
    "exiting",
    "offline",
    "starting",
    "rank down",
    "admin only",
    "system limited",
    "migrating",
    "irrecoverable",
];

/// The message of a request sent to a host manager other than the head's.
const HEAD_HOST_MANAGER_ONLY_MESSAGE: &str = "only accessible from head host manager";

/// Messages reporting that a rank failed while handling the request, besides
/// `Rank <n> non-responsive`; see [`is_rank_failure_message`].
const RANK_FAILURE_MESSAGES: [&str; 2] =
    ["Failed to send insert request to other rank", "failed to process MH insert redirect"];

/// The reason phrase, in lower case, of a `503` for a failure sending between
/// ranks.
const RANK_FAILURE_REASON: &str = "network error";

/// Messages saying a rank is still rebuilding its HA client after its cluster
/// came back online. The rank refuses the request before handling it, so
/// nothing was stored; the state clears within seconds. `Draining HA queue`
/// shares their prefix but is a backlog the cluster must replay first, so it
/// fails over instead.
const RANK_NOT_READY_MESSAGES: [&str; 2] = ["Unavailable: Producer not available", "Unavailable: Consumer not created"];

/// What a rank reports, as a per-record error or a warning for the batch,
/// when it skips a record whose primary key matches one already stored.
const PRIMARY_KEY_COLLISION_MESSAGE: &str = "Primary Key already exists";

/// The wait before the first re-send of a request a rank was not ready for.
const FIRST_RESEND_WAIT: Duration = Duration::from_secs(1);
/// The longest wait before a re-send of a request a rank was not ready for.
const MAX_RESEND_WAIT: Duration = Duration::from_secs(8);

/// What the client does with a reply that failed, given what the server said
/// and the reply's status: its message decides first, then its status.
///
/// `message` is the reply's message, in whichever encoding the reply used, or
/// `None` where it has none.
pub(crate) fn of_reply(status: u16, reason: &str, message: Option<&str>) -> Reply {
    if message.is_some_and(is_rank_not_ready_message) {
        return Reply::RankNotReady;
    }
    if is_server_state(status, reason, message) {
        return Reply::FailOver;
    }
    if message.is_some_and(|m| m.contains(HEAD_HOST_MANAGER_ONLY_MESSAGE)) {
        return Reply::Report;
    }
    if is_rank_failure(status, reason, message) {
        return Reply::RankFailure;
    }
    match status {
        502..=504 => Reply::Resend,
        500 => Reply::FailOver,
        _ => Reply::Report,
    }
}

/// The error for a reply that failed, as [`of_reply`] decides what the client
/// does with it: a [`KineticaError::Exit`], which fails over, for a reply
/// the client fails over from; otherwise a [`KineticaError::Unauthorized`]
/// for a `401` or `403`, and a [`KineticaError::Api`] reporting it to the
/// caller. `description` is the text it is reported with.
///
/// The HTTP layer has already made whatever re-sends the reply allowed, so
/// what is left is to fail over or to report.
pub(crate) fn reply_failure(
    url: &str,
    status: u16,
    reason: &str,
    message: Option<&str>,
    description: String,
) -> KineticaError {
    let reply = of_reply(status, reason, message);
    log::debug!("Reply from {url}; response_code: {status}; reason: {reason}; message: {message:?}; action: {reply:?}");
    match reply {
        Reply::Report if status == 401 || status == 403 => {
            KineticaError::Unauthorized(format!("Server response status: {status} : {reason}"))
        }
        Reply::Report => KineticaError::Api(description),
        Reply::Resend | Reply::RankNotReady | Reply::FailOver | Reply::RankFailure => KineticaError::Exit(description),
    }
}

/// The text a reply with a text body is reported with, such as a gateway's
/// error page: its reason phrase for a server error, or a note that the
/// reply could not be read otherwise.
pub(crate) fn text_reply_description(status: u16, reason: &str) -> String {
    if status >= 500 {
        reason.to_string()
    } else {
        format!("Cannot parse response from server: '{reason}'; status code: {status}")
    }
}

/// Whether a server message says the rank's HA client is not ready yet.
pub(crate) fn is_rank_not_ready_message(message: &str) -> bool {
    RANK_NOT_READY_MESSAGES.iter().any(|m| message.contains(m))
}

/// Whether a reply reports a server state that a second copy of the request
/// would meet again.
fn is_server_state(status: u16, reason: &str, message: Option<&str>) -> bool {
    if message.is_some_and(|m| SERVER_STATE_MESSAGES.iter().any(|state| m.contains(state))) {
        return true;
    }
    status == 503 && SERVER_STATE_REASONS.contains(&reason.trim().to_lowercase().as_str())
}

/// Whether a reply reports that a rank failed while handling the request.
fn is_rank_failure(status: u16, reason: &str, message: Option<&str>) -> bool {
    if message.is_some_and(is_rank_failure_message) {
        return true;
    }
    status == 503 && reason.trim().eq_ignore_ascii_case(RANK_FAILURE_REASON)
}

/// Whether a message reports a rank that failed while handling the request:
/// `Rank <n> non-responsive`, or a failure sending between ranks.
fn is_rank_failure_message(message: &str) -> bool {
    if RANK_FAILURE_MESSAGES.iter().any(|m| message.contains(m)) {
        return true;
    }
    message.match_indices("Rank ").any(|(at, prefix)| {
        let rest = &message[at + prefix.len()..];
        let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        digits > 0 && rest[digits..].starts_with(" non-responsive")
    })
}

/// The wait before a re-send of a request a rank was not ready for, after
/// `waits_so_far` such waits: 1 s before the first re-send, twice as long
/// before each after it, and at most 8 s. A rank takes seconds to rebuild
/// its HA client, so the waits are what let a request's budget of re-sends
/// outlast the rebuild.
pub(crate) fn resend_wait(waits_so_far: usize) -> Duration {
    let doublings = u32::try_from(waits_so_far).unwrap_or(u32::MAX).min(3);
    (FIRST_RESEND_WAIT * 2u32.pow(doublings)).min(MAX_RESEND_WAIT)
}

/// Whether a message a rank returned for a record, or for a batch, reports a
/// primary key collision: a record skipped because its primary key matches
/// one already stored.
pub(crate) fn is_primary_key_collision(message: &str) -> bool {
    message.contains(PRIMARY_KEY_COLLISION_MESSAGE)
}

/// How one request is re-sent to the same server, and what its re-sends left
/// behind.
///
/// A request is given a budget of re-sends, and is marked repeatable where a
/// second copy is harmless even if the server applied the first. When a
/// repeatable request is re-sent after a failure that leaves the first copy's
/// outcome unknown, the failure is recorded here, so that whoever sent the
/// request can say which of its results may come from that copy.
#[derive(Debug, Default)]
pub(crate) struct RequestRetries {
    /// The request's own budget of re-sends, or `None` for the connection's
    /// `max_retries`.
    max_retries: Option<usize>,
    repeatable: bool,
    /// How many times the request was re-sent.
    resends: std::sync::atomic::AtomicUsize,
    /// The last failure after which the request was re-sent although an
    /// earlier copy may have been applied.
    uncertain_failure: parking_lot::Mutex<Option<String>>,
}

impl RequestRetries {
    /// A request with its own budget of `max_retries` re-sends, `repeatable`
    /// where a second copy is harmless even if the server applied the first.
    pub(crate) fn new(max_retries: usize, repeatable: bool) -> Self {
        Self { max_retries: Some(max_retries), repeatable, ..Self::default() }
    }

    /// A request with the connection's budget, not repeatable unless its
    /// endpoint is.
    pub(crate) fn connection_default() -> Self {
        Self::default()
    }

    /// How many times the request may be re-sent to the same server, given
    /// the connection's `max_retries`.
    pub(crate) fn budget(&self, connection_max_retries: usize) -> usize {
        self.max_retries.unwrap_or(connection_max_retries)
    }

    /// Whether a second copy of the request is harmless even if the server
    /// applied the first.
    pub(crate) fn is_repeatable(&self) -> bool {
        self.repeatable
    }

    /// Counts a re-send.
    pub(crate) fn note_resent(&self) {
        self.resends.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// How many times the request was re-sent.
    pub(crate) fn resends(&self) -> usize {
        self.resends.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Records that the request is being re-sent after `failure`, which
    /// leaves an earlier copy's outcome unknown.
    pub(crate) fn note_resent_after_uncertain_failure(&self, failure: String) {
        *self.uncertain_failure.lock() = Some(failure);
    }

    /// The last failure after which the request was re-sent although an
    /// earlier copy may have been applied, or `None` where it never was.
    pub(crate) fn uncertain_failure(&self) -> Option<String> {
        self.uncertain_failure.lock().clone()
    }
}

/// The message of a reply that failed, read from its body by its content
/// type, or `None` for a reply that did not fail.
///
/// - A text reply (a gateway's error page, say) always failed; its message is
///   the start of its text.
/// - A JSON reply failed where its status is not a success; its message is
///   its `message` field. A JSON `ERROR` with a success status is the
///   caller's to read, unless it says the rank's HA client is not ready.
/// - Any other reply is an Avro response wrapper, which failed where its
///   `status` is `ERROR`; its message is its `message`. A body that is empty,
///   or does not start with a wrapper, failed where the status is not a
///   success, with no message.
pub(crate) fn failed_reply_message(status: u16, content_type: Option<&str>, body: &[u8]) -> Option<Option<String>> {
    let success = (200..300).contains(&status);
    let content_type = content_type.unwrap_or("");
    if content_type.starts_with("text") {
        return Some(Some(text_reply_message(body)));
    }
    if content_type.contains("json") {
        #[derive(serde::Deserialize)]
        struct Head {
            status: Option<String>,
            message: Option<String>,
        }
        // A rank that is not ready refuses the request whatever the status;
        // an answer that large is records, not a refusal
        if success && body.len() > JSON_REFUSAL_LIMIT {
            return None;
        }
        let head = serde_json::from_slice::<Head>(body).ok();
        if success {
            return head
                .filter(|head| head.status.as_deref() == Some("ERROR"))
                .and_then(|head| head.message)
                .filter(|message| is_rank_not_ready_message(message))
                .map(Some);
        }
        return Some(head.and_then(|head| head.message));
    }
    match avro_wrapper_head(body) {
        Some(("ERROR", message)) => Some(Some(message.into_owned())),
        Some(_) => None,
        None if success => None,
        None => Some(None),
    }
}

/// The largest JSON answer with a success status that is read for a rank
/// refusing the request.
const JSON_REFUSAL_LIMIT: usize = 64 * 1024;

/// How much of a text reply is taken as its message; a gateway's error page
/// or a short server message is well within it.
const TEXT_REPLY_MESSAGE_LIMIT: usize = 4096;

/// The start of a text reply's body, as its message.
pub(crate) fn text_reply_message(body: &[u8]) -> String {
    String::from_utf8_lossy(&body[..body.len().min(TEXT_REPLY_MESSAGE_LIMIT)]).into_owned()
}

/// The `status` and `message` an Avro response wrapper starts with, read
/// without decoding the rest of it, or `None` where the body does not start
/// with two strings.
fn avro_wrapper_head(body: &[u8]) -> Option<(&str, std::borrow::Cow<'_, str>)> {
    let (status, rest) = avro_string(body)?;
    let (message, _) = avro_string(rest)?;
    Some((std::str::from_utf8(status).ok()?, String::from_utf8_lossy(message)))
}

/// An Avro string at the start of `bytes`, and what follows it.
fn avro_string(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let mut value: u64 = 0;
    for (i, byte) in bytes.iter().take(10).enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            // Zigzag-decoded length
            let length = (value >> 1) as i64 ^ -((value & 1) as i64);
            let length = usize::try_from(length).ok()?;
            let rest = &bytes[i + 1..];
            return (length <= rest.len()).then(|| rest.split_at(length));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_message_decides_before_the_status() {
        // A 503 meaning offline fails over; one meaning busy is re-sent
        assert_eq!(of_reply(503, "Service Unavailable", Some("Kinetica is offline")), Reply::FailOver);
        assert_eq!(of_reply(503, "Service Unavailable", Some("busy")), Reply::Resend);
        assert_eq!(of_reply(503, "Service Unavailable", None), Reply::Resend);
        // Not ready, whatever the status
        for status in [200, 400, 500, 503] {
            assert_eq!(of_reply(status, "", Some("[HA] Unavailable: Producer not available")), Reply::RankNotReady);
        }
        // Draining shares the prefix, but fails over
        assert_eq!(of_reply(400, "Bad Request", Some("Unavailable: Draining HA queue")), Reply::FailOver);
    }

    #[test]
    fn every_server_state_fails_over() {
        for message in SERVER_STATE_MESSAGES {
            assert_eq!(of_reply(400, "Bad Request", Some(&format!("Error: {message}."))), Reply::FailOver, "{message}");
        }
        for reason in SERVER_STATE_REASONS {
            assert_eq!(of_reply(503, &reason.to_uppercase(), None), Reply::FailOver, "{reason}");
            // Only on a 503
            assert_eq!(of_reply(400, reason, None), Reply::Report, "{reason}");
        }
    }

    #[test]
    fn a_rank_failure_is_re_sent_only_if_repeatable() {
        for message in ["Rank 3 non-responsive", "x: Rank 12 non-responsive; y", "Failed to send insert request to other rank"] {
            assert_eq!(of_reply(400, "Bad Request", Some(message)), Reply::RankFailure, "{message}");
        }
        assert_eq!(of_reply(503, " Network Error ", None), Reply::RankFailure);
        for message in ["Rank non-responsive", "Rank x non-responsive", "Rank 3 is fine"] {
            assert_eq!(of_reply(400, "Bad Request", Some(message)), Reply::Report, "{message}");
        }
        assert_eq!(Reply::RankFailure.resend(), Some(Resend::IfRepeatable));
    }

    #[test]
    fn statuses_decide_what_no_message_does() {
        assert_eq!(of_reply(502, "Bad Gateway", Some("x")), Reply::Resend);
        assert_eq!(of_reply(504, "Gateway Timeout", Some("x")), Reply::Resend);
        assert_eq!(of_reply(500, "Internal Server Error", Some("x")), Reply::FailOver);
        assert_eq!(of_reply(400, "Bad Request", Some("x")), Reply::Report);
        assert_eq!(of_reply(401, "Unauthorized", None), Reply::Report);
        // Reported, though a 503
        assert_eq!(of_reply(503, "", Some("only accessible from head host manager")), Reply::Report);
    }

    #[test]
    fn the_wait_doubles_to_8_seconds() {
        let waits: Vec<u64> = (0..6).map(|n| resend_wait(n).as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 8, 8]);
        assert_eq!(resend_wait(usize::MAX).as_secs(), 8);
    }

    #[test]
    fn a_requests_budget_is_its_own_or_the_connections() {
        assert_eq!(RequestRetries::connection_default().budget(3), 3);
        assert_eq!(RequestRetries::new(7, true).budget(3), 7);
        assert_eq!(RequestRetries::new(0, false).budget(3), 0);
        let retries = RequestRetries::new(1, true);
        assert!(retries.is_repeatable());
        assert_eq!(retries.uncertain_failure(), None);
        retries.note_resent_after_uncertain_failure("lost".into());
        retries.note_resent();
        assert_eq!((retries.uncertain_failure().as_deref(), retries.resends()), (Some("lost"), 1));
    }

    /// An Avro response wrapper's start: its status and message.
    fn wrapper(status: &str, message: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for s in [status, message] {
            body.push((s.len() as u8) << 1);
            body.extend_from_slice(s.as_bytes());
        }
        body.extend_from_slice(&[0, 0]);
        body
    }

    #[test]
    fn a_replys_message_is_read_by_its_content_type() {
        let avro = Some("application/octet-stream");
        assert_eq!(failed_reply_message(200, avro, &wrapper("OK", "")), None);
        assert_eq!(failed_reply_message(400, avro, &wrapper("ERROR", "bad")), Some(Some("bad".into())));
        // An ERROR is a failure whatever the status; an OK is not
        assert_eq!(failed_reply_message(200, avro, &wrapper("ERROR", "x")), Some(Some("x".into())));
        assert_eq!(failed_reply_message(503, avro, &wrapper("OK", "")), None);
        // No wrapper
        assert_eq!(failed_reply_message(503, avro, b""), Some(None));
        assert_eq!(failed_reply_message(200, avro, b""), None);
        assert_eq!(failed_reply_message(502, None, &[0xff]), Some(None));

        let json = Some("application/json");
        assert_eq!(failed_reply_message(200, json, br#"{"status":"ERROR","message":"m"}"#), None);
        let not_ready = br#"{"status":"ERROR","message":"Unavailable: Consumer not created"}"#;
        assert_eq!(failed_reply_message(200, json, not_ready), Some(Some("Unavailable: Consumer not created".into())));
        assert_eq!(failed_reply_message(400, json, br#"{"status":"ERROR","message":"m"}"#), Some(Some("m".into())));
        assert_eq!(failed_reply_message(503, json, b"not json"), Some(None));

        let text = Some("text/html");
        assert_eq!(failed_reply_message(200, text, b"<html/>"), Some(Some("<html/>".into())));
        let long = vec![b'x'; TEXT_REPLY_MESSAGE_LIMIT + 10];
        assert_eq!(failed_reply_message(502, text, &long).unwrap().unwrap().len(), TEXT_REPLY_MESSAGE_LIMIT);
    }

    #[test]
    fn a_wrapper_with_a_long_message_is_read() {
        let message = "m".repeat(300);
        let mut body = vec![10];
        body.extend_from_slice(b"ERROR");
        // 300, zigzag-encoded as 600: two bytes
        body.extend_from_slice(&[0xd8, 0x04]);
        body.extend_from_slice(message.as_bytes());
        assert_eq!(failed_reply_message(400, None, &body), Some(Some(message)));
        // Cut short
        assert_eq!(failed_reply_message(400, None, &body[..20]), Some(None));
    }
}
