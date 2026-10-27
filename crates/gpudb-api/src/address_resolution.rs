//! Turns a cluster's system properties into the addresses a client will use.
//!
//! Everything here is a **pure function of its arguments**: a properties map, a
//! hostname pattern, a list of candidates. Nothing contacts a server, and
//! nothing reads connection state. Address resolution has a parse phase and a
//! select phase that can be exercised exhaustively with no cluster, and a probe
//! phase that cannot; the probe phase stays on [`Kinetica`](crate::Kinetica),
//! where the connection lives.
//!
//! The exception is whether an address names this machine
//! ([`is_local_address`] and the credibility filters built on it), which
//! resolves host names and reads the local interfaces. It contacts no server
//! either.
//!
//! Two notes apply throughout:
//!
//! - Lists are split by [`split_dropping_trailing_empty`], which drops
//!   trailing empty fields and returns the whole string when the separator
//!   does not occur, so that, for one, a removed *last* rank compacts away.
//! - Addresses are parsed with the `url` crate, which lowercases host names
//!   and normalizes an empty path to `/`, so `http://h:9191` and
//!   `http://h:9191/` are the same address here.

use std::collections::{HashMap, HashSet};

use log::{debug, warn};
use url::Url;

use crate::KineticaError;

// ------------------------------------------------------------------
// The property names this module reads
// ------------------------------------------------------------------

pub(crate) const SYSTEM_PROPERTIES_RESPONSE_RING_NAME: &str = "conf.ring_name";
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_CLUSTER_NAME: &str = "conf.cluster_name";
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_ENABLE_HTTPD: &str = "conf.enable_httpd_proxy";
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_ENABLE_MH: &str = "conf.enable_worker_http_servers";
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS: &str = "conf.number_of_hosts";
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_HEAD_NODE_URLS: &str = "conf.ha_ring_head_nodes_full";
/// A semicolon-separated list of rank URL entries for the cluster; each entry
/// is either an admin-configured URL or a list of host interface URLs.
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS: &str = "conf.worker_http_server_urls";
/// A semicolon-separated list of rank URLs for the cluster; each entry is
/// generated using the host's configured internal address.
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS_PRIVATE: &str = "conf.worker_http_server_urls_private";
/// The host interface addresses each rank's host carries.
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_SERVER_IPS: &str = "conf.worker_http_server_ips";
/// The ports to be matched with the entries of the interface addresses.
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_SERVER_PORTS: &str = "conf.worker_http_server_ports";
/// Scheme of a rank's own listener.
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_USE_HTTPS: &str = "conf.use_https";

/// `conf.host<i>_host_manager_public_url`: the address an operator has
/// published for a host's host manager. All-or-none across hosts and unique
/// per host, so where it is set at all it is a complete table.
fn hm_public_url_key(host: i32) -> String {
    format!("conf.host{host}_host_manager_public_url")
}

/// The port the host manager listens on.
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_HM_PORT: &str = "conf.hm_http_port";
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_ENABLE_HA: &str = "conf.enable_ha";
pub(crate) const SYSTEM_PROPERTIES_RESPONSE_TRUE: &str = "TRUE";

/// Per-rank candidate addresses, indexed by rank slot (entry 0 is the head
/// rank). `None` is a rank removed from the cluster, whose slot is kept so the
/// indices stay rank numbers.
pub(crate) type RankCandidates = Vec<Option<Vec<Url>>>;

/// Splits `text` on `separator`: trailing empty fields are dropped, and text
/// with no separator in it is returned whole, even when empty.
pub(crate) fn split_dropping_trailing_empty<'a>(text: &'a str, separator: &str) -> Vec<&'a str> {
    if !text.contains(separator) {
        return vec![text];
    }
    let mut fields: Vec<&str> = text.split(separator).collect();
    while fields.last().is_some_and(|f| f.is_empty()) {
        fields.pop();
    }
    fields
}

/// The user-given hostname regex, kept with the pattern as given.
///
/// Matching is a **prefix** match: the pattern is anchored at the start of
/// the candidate and unanchored at the end, so `172\.17\.` selects every
/// address in that range without a trailing `.*`. The consequence is that a
/// pattern is *not* implicitly exact: `10\.0\.0\.1` also selects
/// `10.0.0.10`. A pattern meant to name one address must end in `$`.
///
/// Rust's `Regex::is_match` searches anywhere in the text, so the anchoring is
/// added at compile time; the pattern is kept separately so that messages show
/// it as given.
///
/// The pattern is read by `fancy-regex`, which takes look-ahead,
/// look-behind, back-references and atomic groups, and runs a pattern without
/// them on the `regex` crate's linear-time engine. A match that runs past its
/// backtracking limit counts as no match.
#[derive(Debug, Clone)]
pub(crate) struct HostnameRegex {
    pattern: String,
    regex: fancy_regex::Regex,
}

impl HostnameRegex {
    /// Compiles `pattern`, or `None` for an empty one, which every resolver
    /// treats as no pattern at all.
    pub(crate) fn compile(pattern: &str) -> Result<Option<Self>, KineticaError> {
        if pattern.is_empty() {
            return Ok(None);
        }
        // The pattern is wrapped in a non-capturing group so that a top-level
        // alternation binds as written: `^(?:a|b)`, not `^a|b`.
        fancy_regex::Regex::new(&format!("^(?:{})", literal_quotes_escaped(pattern)))
            .map(|regex| Some(Self { pattern: pattern.to_string(), regex }))
            .map_err(|e| {
                KineticaError::Api(format!(
                    "Error in parsing the pattern for hostname regex; given: '{pattern}'; got error: {e}"
                ))
            })
    }

    /// The pattern as the user gave it.
    pub(crate) fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Whether the pattern matches a prefix of `candidate`.
    pub(crate) fn matches(&self, candidate: &str) -> bool {
        self.regex.is_match(candidate).unwrap_or(false)
    }
}

/// `pattern` with each `\Q...\E` quotation, literal text up to `\E` or the
/// pattern's end, written as escaped characters instead, as `fancy-regex` has
/// no quotation.
fn literal_quotes_escaped(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut rest = pattern;
    while let Some(at) = rest.find('\\') {
        out.push_str(&rest[..at]);
        let escape = &rest[at..];
        if let Some(quoted) = escape.strip_prefix("\\Q") {
            let (literal, after) = quoted.split_once("\\E").unwrap_or((quoted, ""));
            out.push_str(&fancy_regex::escape(literal));
            rest = after;
        } else {
            // Any other escape, an escaped backslash included, as it is
            let length = escape.chars().nth(1).map_or(1, |c| 1 + c.len_utf8());
            out.push_str(&escape[..length]);
            rest = &escape[length..];
        }
    }
    out.push_str(rest);
    out
}

impl std::fmt::Display for HostnameRegex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.pattern)
    }
}

/// The host-manager path for a head node URL reached through httpd: the head
/// node's own path with its *last segment replaced* by `gpudb-host-manager`.
pub(crate) fn host_manager_path_for(head_node_url: &Url) -> String {
    let path = head_node_url.path();
    match path.rfind('/') {
        None => "/gpudb-host-manager".to_string(),
        Some(last_slash) => format!("{}gpudb-host-manager", &path[..=last_slash]),
    }
}

/// The host manager address the deployment publishes for the host that
/// `head_node_url` names, or `None` where none is published.
///
/// Matched by host rather than by index, because what is held here is an
/// address while the property is indexed by host number. `None` where the
/// property is unset, unparsable, or set for other hosts only.
pub(crate) fn published_host_manager_url(
    system_properties: &HashMap<String, String>,
    head_node_url: &Url,
) -> Option<Url> {
    let host_count = int_property(system_properties, SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS, 0);

    for host in 0..host_count {
        let Some(value) = system_properties.get(&hm_public_url_key(host)) else {
            continue;
        };
        if value.trim().is_empty() {
            continue;
        }
        match Url::parse(value.trim()) {
            Ok(candidate) => {
                if candidate.host_str() == head_node_url.host_str() {
                    return Some(candidate);
                }
            }
            Err(e) => debug!("Ignoring malformed published host manager URL <{value}>: {e}"),
        }
    }

    None
}

/// An integer property, or `fallback` where it is absent or not a number.
fn int_property(system_properties: &HashMap<String, String>, key: &str, fallback: i32) -> i32 {
    system_properties
        .get(key)
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(fallback)
}

/// The host manager port the deployment reports, or `fallback`. The fallback
/// is normally the caller's configured port, which is why it is not defaulted
/// here.
pub(crate) fn published_host_manager_port(system_properties: &HashMap<String, String>, fallback: i32) -> i32 {
    int_property(system_properties, SYSTEM_PROPERTIES_RESPONSE_HM_PORT, fallback)
}

/// A value that differs between two clusters whose configurations differ,
/// built from the ring name, the cluster name, and the public and private rank
/// URLs.
///
/// **One-sided, and that is the whole of what it claims.** Two signatures
/// differing proves the addresses belong to different clusters. Two matching
/// proves nothing: no property is guaranteed to differ, the ones that do are
/// addresses, and two deployments built from one configuration are identical
/// in every value a client can see. Use it to reject, never to confirm.
///
/// Empty where none of those properties is set.
pub(crate) fn cluster_identity_signature(system_properties: &HashMap<String, String>) -> String {
    let mut signature = String::new();

    for key in [
        SYSTEM_PROPERTIES_RESPONSE_RING_NAME,
        SYSTEM_PROPERTIES_RESPONSE_CLUSTER_NAME,
        SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS,
        SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS_PRIVATE,
    ] {
        if let Some(value) = system_properties.get(key) {
            if !value.trim().is_empty() {
                signature.push_str(&format!("{key}={}\n", value.trim()));
            }
        }
    }

    signature
}

/// Whether an address names this machine.
///
/// Used to decide whether an address a server advertises could refer to that
/// server from here. The question is not whether the caller wrote a loopback
/// URL -- addressing one's own machine by a routable address is an ordinary
/// choice -- but whether the server is on this machine at all.
///
/// `host` is a host name or literal address, as [`Url::host_str`] gives it.
/// It is resolved first and the addresses are tested, so a host alias
/// classifies the same as the literal it names; every resolved address is
/// considered, because a name may carry several and only one need be local.
/// True if any is loopback or sits on a local interface; false if it cannot be
/// resolved.
pub(crate) async fn is_local_address(host: &str) -> bool {
    if host.is_empty() {
        return false;
    }

    // A literal needs no lookup; an IPv6 one arrives in brackets
    let literal = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
    let addresses: Vec<std::net::IpAddr> = match literal.parse::<std::net::IpAddr>() {
        Ok(address) => vec![address],
        Err(_) => match tokio::net::lookup_host((literal, 0)).await {
            Ok(resolved) => resolved.map(|socket| socket.ip()).collect(),
            Err(e) => {
                debug!("Could not classify <{host}> as local: {e}");
                return false;
            }
        },
    };

    // An IPv4-mapped address is taken as the IPv4 one
    let addresses: Vec<std::net::IpAddr> = addresses.into_iter().map(|a| a.to_canonical()).collect();
    if addresses.iter().any(std::net::IpAddr::is_loopback) {
        return true;
    }

    match if_addrs::get_if_addrs() {
        Ok(interfaces) => interfaces.iter().any(|interface| addresses.contains(&interface.ip())),
        Err(e) => {
            debug!("Could not classify <{host}> as local: {e}");
            false
        }
    }
}

/// Whether an advertised address is one this connection may use.
///
/// Rejects exactly one case: the server was reached on another machine, and
/// `url` names the machine this client is running on -- loopback, or any
/// address on a local interface. Such an address cannot be the server. A probe
/// sent there reaches whatever is listening locally, and that answers
/// indistinguishably from the cluster that was asked for.
///
/// `None` (a rank removed from the cluster) is accepted, and everything is
/// accepted where `server_is_local`. `locality_cache` holds the verdicts
/// reached so far, by host name, so that a host repeated across ranks costs
/// one lookup.
async fn is_credible_address(url: Option<&Url>, server_is_local: bool, locality_cache: &mut HashMap<String, bool>) -> bool {
    let Some(url) = url else {
        return true;
    };
    if server_is_local {
        return true;
    }

    let host = url.host_str().unwrap_or("").to_string();
    let local = match locality_cache.get(&host) {
        Some(&local) => local,
        None => {
            let local = is_local_address(&host).await;
            locality_cache.insert(host, local);
            local
        }
    };

    !local
}

/// The rank addresses, or an empty list where any of them could not name this
/// server.
///
/// **All or nothing, deliberately.** The list is index-aligned -- entry `i` is
/// rank `i`, and `None` is a rank removed from the cluster. Dropping an entry
/// would shift every rank above it, which silently misroutes sharded records,
/// and `None` cannot stand in for "not credible", because it already means
/// something else. So one incredible address withdraws the whole list, and
/// multi-head with it: the alternative is routing shard keys at an address
/// that may be a different cluster.
///
/// Returned unchanged where `server_is_local`, or where `urls` is empty.
pub(crate) async fn rank_urls_if_all_credible(
    urls: Vec<Option<Url>>,
    server_is_local: bool,
    _what: &str,
) -> Vec<Option<Url>> {
    if server_is_local || urls.is_empty() {
        return urls;
    }

    let mut locality_cache = HashMap::new();
    for url in &urls {
        if !is_credible_address(url.as_ref(), server_is_local, &mut locality_cache).await {
            // WARN, not debug: this costs the caller multi-head, and the
            // symptom otherwise is a silent throughput cliff that looks exactly
            // like a cluster with no worker servers configured.
            warn!(
                "Multi-head operations disabled for this cluster: it advertises <{}> for a rank, which names \
                 this machine and so cannot name a server reached elsewhere.  Requests will go through the head \
                 node.  The advertised addresses are rank-indexed, so the rest cannot be used without this one.",
                url.as_ref().map(Url::as_str).unwrap_or("null")
            );
            return Vec::new();
        }
    }

    urls
}

/// The candidates that could name this server, from one rank's alternatives.
///
/// Safe to filter, unlike [`rank_urls_if_all_credible`]: these are
/// alternative addresses for one rank rather than one address per rank, so
/// removing some reorders nothing and loses nothing but a probe that could only
/// have reached the wrong machine. The original order is kept; the result may
/// be empty. `None` (a removed rank) is returned unchanged, and so is
/// everything where `server_is_local`.
pub(crate) async fn retain_credible_urls(
    urls: Option<Vec<Url>>,
    server_is_local: bool,
    what: &str,
) -> Option<Vec<Url>> {
    let urls = urls?;
    if server_is_local {
        return Some(urls);
    }

    let mut locality_cache = HashMap::new();
    let mut retained = Vec::with_capacity(urls.len());
    for url in urls {
        if is_credible_address(Some(&url), server_is_local, &mut locality_cache).await {
            retained.push(url);
        } else {
            debug!("Skipping {what} <{url}>: it names this machine, not a server reached elsewhere");
        }
    }

    Some(retained)
}

fn is_true(system_properties: &HashMap<String, String>, key: &str) -> bool {
    system_properties
        .get(key)
        .is_some_and(|v| v.eq_ignore_ascii_case(SYSTEM_PROPERTIES_RESPONSE_TRUE))
}

/// Whether the cluster reports an httpd proxy.
pub(crate) fn is_httpd_enabled(system_properties: &HashMap<String, String>) -> bool {
    is_true(system_properties, SYSTEM_PROPERTIES_RESPONSE_ENABLE_HTTPD)
}

/// Whether the cluster has multi-head I/O enabled.
pub(crate) fn is_multi_head_enabled(system_properties: &HashMap<String, String>) -> bool {
    is_true(system_properties, SYSTEM_PROPERTIES_RESPONSE_ENABLE_MH)
}

/// Whether a parsed address names a host that can be connected to.
pub(crate) fn has_usable_host(url: &Url) -> bool {
    url.host_str().is_some_and(|host| !host.is_empty())
}

/// Whether a host, as bare text, names something that can be connected to:
/// the string-level counterpart of [`has_usable_host`], rejecting an empty
/// entry and one that carries a port but no host.
pub(crate) fn has_usable_host_text(host: &str) -> bool {
    !host.is_empty() && !host.starts_with(':')
}

/// The host component of an advertised address, whatever form it arrives in,
/// so that every resolver matches the hostname regex against the same thing. A
/// scheme is supplied when the text has none. `None` if the text cannot be
/// parsed as an address.
pub(crate) fn host_of(address: &str) -> Option<String> {
    let with_scheme = if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    };
    Url::parse(&with_scheme).ok().and_then(|url| url.host_str().map(str::to_string))
}

/// Names a rank for a user-facing message; rank 0 is the head.
pub(crate) fn rank_label(rank_index: usize) -> String {
    if rank_index == 0 {
        "the head rank".to_string()
    } else {
        format!("worker rank {rank_index}")
    }
}

/// The caller-given URLs whose host the hostname regex accepts, in the order
/// given; with no pattern, all of them.
///
/// Asked when the regex matches nothing the server advertises, to decide
/// whether the connection can continue head-node-only rather than fail.
pub(crate) fn urls_accepted_by_hostname_regex(urls: &[Url], hostname_regex: Option<&HostnameRegex>) -> Vec<Url> {
    let Some(regex) = hostname_regex else {
        return urls.to_vec();
    };
    urls.iter()
        .filter(|url| has_usable_host(url) && regex.matches(url.host_str().unwrap_or("")))
        .cloned()
        .collect()
}

/// Extracts the head and worker rank URLs: one per rank slot, entry 0 the head
/// rank, `None` for a removed rank. Empty if multi-head is off or the server
/// advertises no rank addresses.
pub(crate) fn extract_rank_urls(
    system_properties: &HashMap<String, String>,
    hostname_regex: Option<&HostnameRegex>,
) -> Result<Vec<Option<Url>>, KineticaError> {
    select_rank_urls(&extract_rank_candidate_urls(system_properties), hostname_regex)
}

/// Splits one comma-separated alternate list into the usable addresses, in
/// advertised order; an unusable one is skipped, not fatal.
pub(crate) fn parse_usable_alternate_urls(alternate_list: &str, what: &str) -> Vec<Url> {
    let mut usable = Vec::new();
    for url_string in split_dropping_trailing_empty(alternate_list, ",") {
        let url = match Url::parse(url_string) {
            Ok(url) => url,
            Err(e) => {
                debug!("Skipping unusable {what} <{url_string}>: {e}");
                continue;
            }
        };
        if !has_usable_host(&url) {
            debug!("Skipping {what} naming no host <{url_string}>");
            continue;
        }
        usable.push(url);
    }
    usable
}

/// Keeps the candidates whose host the regex selects, in the order given; with
/// no pattern, all of them. A filter, not a selector: every match is kept.
pub(crate) fn filter_urls_by_hostname_regex(
    candidates: &[Url],
    hostname_regex: Option<&HostnameRegex>,
    what: &str,
) -> Vec<Url> {
    let Some(regex) = hostname_regex else {
        return candidates.to_vec();
    };
    let mut retained = Vec::new();
    for url in candidates {
        if regex.matches(url.host_str().unwrap_or("")) {
            debug!("Keeping matching {what}: {url}");
            retained.push(url.clone());
        } else {
            debug!("Skipping non-matching {what}: {url}");
        }
    }
    retained
}

/// Splits a `;`-separated per-rank property into its slot fields; empty if the
/// property is absent or empty. A removed rank is an empty field.
pub(crate) fn extract_rank_slot_fields<'a>(
    system_properties: &'a HashMap<String, String>,
    key: &str,
) -> Vec<&'a str> {
    match system_properties.get(key) {
        Some(value) if !value.is_empty() => split_dropping_trailing_empty(value, ";"),
        _ => Vec::new(),
    }
}

/// The field for a rank slot, or `""` where the property does not reach it.
pub(crate) fn rank_slot_field<'a>(fields: &[&'a str], slot: usize) -> &'a str {
    fields.get(slot).copied().unwrap_or("")
}

/// Builds one rank's direct-form candidates from its host's interface
/// addresses, which arrive bare: the scheme is the rank's own (`conf.use_https`),
/// not the one dressing the advertised URLs, and the port is the rank's. An
/// IPv6 literal is skipped rather than emitted unbracketed.
pub(crate) fn build_enumerated_candidate_urls(ips_field: &str, port_field: &str, use_https: bool) -> Vec<Url> {
    let mut built = Vec::new();
    if ips_field.is_empty() || port_field.is_empty() {
        return built;
    }
    let scheme = if use_https { "https" } else { "http" };
    for address in split_dropping_trailing_empty(ips_field, ",") {
        if address.is_empty() {
            continue;
        }
        if address.contains(':') {
            debug!("Skipping enumerated address needing IPv6 bracketing <{address}>");
            continue;
        }
        let url_string = format!("{scheme}://{address}:{port_field}");
        match Url::parse(&url_string) {
            Ok(url) => {
                if has_usable_host(&url) {
                    built.push(url);
                }
            }
            Err(e) => debug!("Skipping unbuildable enumerated address <{url_string}>: {e}"),
        }
    }
    built
}

/// Concatenates one rank's candidates in intent order, dropping duplicates.
///
/// Configured addresses precede discovered ones, and intent is a property of
/// the entry rather than of the property it arrived in: an advertised address
/// whose host is also enumerated was discovered, any other was configured. So
/// the order is configured advertised, private, discovered advertised, then
/// enumerated.
pub(crate) fn order_rank_candidate_urls(advertised: Vec<Url>, private_urls: Vec<Url>, enumerated: Vec<Url>) -> Vec<Url> {
    let enumerated_hosts: HashSet<String> =
        enumerated.iter().filter_map(|url| url.host_str().map(str::to_string)).collect();

    let (discovered_advertised, configured_advertised): (Vec<Url>, Vec<Url>) = advertised
        .into_iter()
        .partition(|url| url.host_str().is_some_and(|h| enumerated_hosts.contains(h)));

    let mut ordered = Vec::new();
    let mut seen = HashSet::new();
    for tier in [configured_advertised, private_urls, discovered_advertised, enumerated] {
        for url in tier {
            if seen.insert(url.to_string()) {
                ordered.push(url);
            }
        }
    }
    ordered
}

/// The host names/IPs advertised under more than one host, which identify
/// none of them (a container bridge gateway is the same string on every
/// machine of a fleet). `rank_host` maps a rank slot to its host, or `None`.
pub(crate) fn non_identifying_hosts(candidate_urls: &RankCandidates, rank_host: &[Option<usize>]) -> HashSet<String> {
    let mut first_host_by_address: HashMap<String, usize> = HashMap::new();
    let mut non_identifying = HashSet::new();

    for (rank, candidates) in candidate_urls.iter().enumerate() {
        let Some(candidates) = candidates else {
            continue; // slot kept for a removed rank
        };
        let Some(host) = rank_host.get(rank).copied().flatten() else {
            continue; // ungrouped: it says nothing about any host
        };
        for candidate in candidates {
            let address = candidate.host_str().unwrap_or("").to_string();
            let seen = *first_host_by_address.entry(address.clone()).or_insert(host);
            if seen != host {
                non_identifying.insert(address);
            }
        }
    }

    if !non_identifying.is_empty() {
        debug!(
            "Addresses advertised under more than one host, and so usable only for the rank that named them: {non_identifying:?}"
        );
    }
    non_identifying
}

/// Orders a rank's candidates with those naming a single host first, the
/// ambiguous ones after them in their established order. Applied where the
/// address chosen becomes the one a cluster is addressed by. Ambiguous
/// candidates are deprioritized, never dropped.
pub(crate) fn order_urls_by_unique_hosts_first(candidates: Vec<Url>, non_identifying: &HashSet<String>) -> Vec<Url> {
    if non_identifying.is_empty() {
        return candidates;
    }
    let (ambiguous, mut identifying): (Vec<Url>, Vec<Url>) = candidates
        .into_iter()
        .partition(|url| non_identifying.contains(url.host_str().unwrap_or("")));
    if !ambiguous.is_empty() {
        let shown: Vec<&str> = ambiguous.iter().map(Url::as_str).collect();
        debug!("Head rank addresses naming more than one host, tried last: {shown:?}");
    }
    identifying.extend(ambiguous);
    identifying
}

/// Orders a rank's candidates with those on the host's preferred address
/// first, the rest after them in their established order. Matched on the host
/// alone: the preferred address was proven by a different rank, with its own
/// port and path.
pub(crate) fn order_urls_by_preferred_host_first(candidates: &[Url], preferred_host: Option<&str>) -> Vec<Url> {
    let Some(preferred_host) = preferred_host else {
        return candidates.to_vec();
    };
    let (mut preferred, rest): (Vec<Url>, Vec<Url>) = candidates
        .iter()
        .cloned()
        .partition(|url| url.host_str() == Some(preferred_host));
    preferred.extend(rest);
    preferred
}

/// Maps each rank slot to the host it runs on, by the join
/// `conf.rank<r>_ip_address == conf.host<h>_address` on the raw configured
/// values. `None` where the rank is removed or its host cannot be identified.
pub(crate) fn extract_rank_host_mapping(system_properties: &HashMap<String, String>, rank_slots: usize) -> Vec<Option<usize>> {
    let mut rank_host = vec![None; rank_slots];

    let host_count = match system_properties
        .get(SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS)
        .and_then(|v| v.parse::<i32>().ok())
    {
        Some(count) => count,
        None => {
            debug!("No usable host count; ranks will not be grouped by host");
            return rank_host;
        }
    };

    // A host with no address contributes nothing rather than matching every
    // rank that also has none.
    let mut host_index_by_address: HashMap<&str, usize> = HashMap::new();
    for h in 0..host_count.max(0) as usize {
        if let Some(address) = system_properties.get(&format!("conf.host{h}_address")) {
            if !address.is_empty() {
                host_index_by_address.insert(address, h);
            }
        }
    }

    for (r, slot) in rank_host.iter_mut().enumerate() {
        let Some(address) = system_properties.get(&format!("conf.rank{r}_ip_address")) else {
            continue;
        };
        // A removed rank is an empty value
        if address.is_empty() {
            continue;
        }
        match host_index_by_address.get(address.as_str()) {
            Some(&host) => *slot = Some(host),
            None => debug!(
                "No host matches the address of {} <{address}>; it will not be grouped",
                rank_label(r)
            ),
        }
    }
    rank_host
}

/// Phase 1 of address resolution: each rank's candidate addresses, from the
/// four address properties, in intent order. See [`RankCandidates`] for the
/// three states of a slot; an empty list is a rank that advertised addresses
/// none of which was usable.
pub(crate) fn extract_rank_candidate_urls(system_properties: &HashMap<String, String>) -> RankCandidates {
    let mut candidates = Vec::new();

    if !is_multi_head_enabled(system_properties) {
        debug!("Distributed I/O not enabled on server; skipping rank URL retrieval.");
        return candidates;
    }

    let property_val = match system_properties.get(SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS) {
        Some(v) if !v.is_empty() => v,
        _ => {
            debug!("No entry for <{SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS}> in /show/system/properties response");
            return candidates;
        }
    };

    debug!("Known rank URLs <{SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS}> from server: {property_val}");

    // The other three address properties are emitted together under the same
    // server setting; an absent one (an older server) contributes nothing.
    let advertised_fields = split_dropping_trailing_empty(property_val, ";");
    let private_fields = extract_rank_slot_fields(system_properties, SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS_PRIVATE);
    let ips_fields = extract_rank_slot_fields(system_properties, SYSTEM_PROPERTIES_RESPONSE_SERVER_IPS);
    let port_fields = extract_rank_slot_fields(system_properties, SYSTEM_PROPERTIES_RESPONSE_SERVER_PORTS);
    let use_https = is_true(system_properties, SYSTEM_PROPERTIES_RESPONSE_USE_HTTPS);

    for (i, advertised_field) in advertised_fields.iter().enumerate() {
        // A removed rank: the advertised list is the authority for which slots
        // are vacant, and its slot is kept so indices stay rank numbers.
        if advertised_field.is_empty() {
            candidates.push(None);
            continue;
        }
        let advertised = parse_usable_alternate_urls(advertised_field, "rank URL");
        let private_urls = parse_usable_alternate_urls(rank_slot_field(&private_fields, i), "private rank URL");
        let enumerated = build_enumerated_candidate_urls(
            rank_slot_field(&ips_fields, i),
            rank_slot_field(&port_fields, i),
            use_https,
        );
        candidates.push(Some(order_rank_candidate_urls(advertised, private_urls, enumerated)));
    }
    candidates
}

/// Phase 2 of address resolution: one address per rank slot, the first of its
/// retained candidates, `None` for a removed rank.
pub(crate) fn select_rank_urls(
    candidates: &RankCandidates,
    hostname_regex: Option<&HostnameRegex>,
) -> Result<Vec<Option<Url>>, KineticaError> {
    if let Some(regex) = hostname_regex {
        debug!("Selecting rank URLs against user-given regex: {regex}");
    }
    let mut rank_urls = Vec::new();
    for retained in retain_matching_rank_candidate_urls(candidates, hostname_regex)? {
        match retained {
            None => rank_urls.push(None),
            Some(retained) => {
                // Before any probe has run, the first survivor is the one used;
                // the rest are kept so probing can re-select.
                debug!("Keeping rank URL: {}", retained[0]);
                rank_urls.push(Some(retained[0].clone()));
            }
        }
    }
    Ok(rank_urls)
}

/// Applies the hostname regex to every rank's candidates, keeping all matches,
/// and requires every live rank to keep at least one.
///
/// A rank with nothing usable advertised fails as an ordinary error, so the
/// connection can degrade; a rank whose usable addresses the regex rejected
/// fails as a [`KineticaError::HostnameRegexFailure`].
pub(crate) fn retain_matching_rank_candidate_urls(
    candidates: &RankCandidates,
    hostname_regex: Option<&HostnameRegex>,
) -> Result<RankCandidates, KineticaError> {
    if let Some(regex) = hostname_regex {
        debug!("Selecting rank URLs against user-given regex: {regex}");
    }
    let mut retained_by_rank = Vec::with_capacity(candidates.len());
    for (i, rank_candidates) in candidates.iter().enumerate() {
        let Some(rank_candidates) = rank_candidates else {
            retained_by_rank.push(None);
            continue;
        };
        if rank_candidates.is_empty() {
            return Err(KineticaError::Api(format!("No valid IP/hostname found for {}", rank_label(i))));
        }
        let retained = filter_urls_by_hostname_regex(rank_candidates, hostname_regex, "rank URL");
        if retained.is_empty() {
            return Err(KineticaError::HostnameRegexFailure(format!(
                "No valid matching IP/hostname found for {}",
                rank_label(i)
            )));
        }
        retained_by_rank.push(Some(retained));
    }
    Ok(retained_by_rank)
}

/// The host names or IP addresses of every machine in the cluster, one per
/// host: the first of its public addresses, or with a regex the first it
/// matches. Each keeps any port and path it was advertised with, with the
/// scheme removed.
pub(crate) fn host_names_from_system_properties(
    system_properties: &HashMap<String, String>,
    hostname_regex: Option<&HostnameRegex>,
) -> Result<HashSet<String>, KineticaError> {
    debug!(
        "Extracting server-known host names from system properties{}",
        hostname_regex.map_or(String::new(), |r| format!(" using user-given regex: {r}"))
    );

    let num_hosts_str = system_properties
        .get(SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS)
        .ok_or_else(|| KineticaError::Api(format!("Missing value for {SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS}")))?;
    let num_hosts: i32 = num_hosts_str.parse().map_err(|_| {
        KineticaError::Api(format!(
            "Unparsable entry for '{SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS}' ({num_hosts_str}); need an integer"
        ))
    })?;

    let mut cluster_hostnames = HashSet::new();
    for i in 0..num_hosts.max(0) {
        let hostname_key = format!("conf.host{i}_public_urls");
        let hostname_str = system_properties
            .get(&hostname_key)
            .ok_or_else(|| KineticaError::Api(format!("Missing value for {i}th hostname '{hostname_key}'")))?;

        // Collect the usable ones first, so that "advertised nothing usable"
        // and "the regex rejected what was advertised" stay apart.
        let mut usable_hosts = Vec::new();
        for hostname in split_dropping_trailing_empty(hostname_str, ",") {
            // Strip the protocol
            let split_hostname = split_dropping_trailing_empty(hostname, "://");
            let host = if split_hostname.len() > 1 { split_hostname[1] } else { split_hostname[0] };

            let host_text = host_of(host);
            if !has_usable_host_text(host) || !host_text.as_deref().is_some_and(has_usable_host_text) {
                debug!("Skipping host entry naming no host <{hostname}>");
                continue;
            }
            usable_hosts.push(host);
        }

        if usable_hosts.is_empty() {
            return Err(KineticaError::Api(format!("No matching hostname found for host #{i}.")));
        }

        let mut selected = None;
        for host in usable_hosts {
            let Some(regex) = hostname_regex else {
                debug!("Keeping hostname: {host}");
                selected = Some(host);
                break;
            };
            if regex.matches(&host_of(host).unwrap_or_default()) {
                debug!("Keeping matching hostname: {host}");
                selected = Some(host);
                break;
            }
            debug!("Skipping non-matching hostname: {host}");
        }

        // Every hostname reaching the loop was usable, so nothing selected
        // means the regex is genuinely why.
        let Some(selected) = selected else {
            return Err(KineticaError::HostnameRegexFailure(format!(
                "No matching hostname found for host #{i} (given hostname regex {})",
                hostname_regex.map_or("null".to_string(), ToString::to_string)
            )));
        };
        cluster_hostnames.insert(selected.to_string());
    }
    Ok(cluster_hostnames)
}

/// The head node URLs of the clusters of the HA ring, one per cluster, if HA is
/// enabled: the first usable address of each, or with a regex the first it
/// matches.
pub(crate) fn ha_ring_head_node_urls(
    system_properties: &HashMap<String, String>,
    hostname_regex: Option<&HostnameRegex>,
) -> Result<Vec<Url>, KineticaError> {
    let mut ha_ring_head_node_urls = Vec::new();

    if !is_true(system_properties, SYSTEM_PROPERTIES_RESPONSE_ENABLE_HA) {
        return Ok(ha_ring_head_node_urls);
    }
    let ha_ring_head_nodes = match system_properties.get(SYSTEM_PROPERTIES_RESPONSE_HEAD_NODE_URLS) {
        Some(v) if !v.is_empty() => v,
        _ => return Ok(ha_ring_head_node_urls),
    };

    for cluster_urls in split_dropping_trailing_empty(ha_ring_head_nodes, ";") {
        let cluster_candidates = parse_usable_alternate_urls(cluster_urls, "head node URL");
        if cluster_candidates.is_empty() {
            return Err(KineticaError::Api(format!(
                "No matching IP/hostname found for cluster with head node URLs {cluster_urls}"
            )));
        }
        let retained = filter_urls_by_hostname_regex(&cluster_candidates, hostname_regex, "head node URL");
        let Some(first) = retained.into_iter().next() else {
            return Err(KineticaError::HostnameRegexFailure(format!(
                "No matching IP/hostname found for cluster with head node URLs {cluster_urls} (given hostname regex {})",
                hostname_regex.map_or("null".to_string(), ToString::to_string)
            )));
        };
        debug!("Keeping head node URL: {first}");
        ha_ring_head_node_urls.push(first);
    }
    Ok(ha_ring_head_node_urls)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        let mut p = HashMap::from([(SYSTEM_PROPERTIES_RESPONSE_ENABLE_MH.to_string(), "TRUE".to_string())]);
        for (k, v) in pairs {
            p.insert((*k).to_string(), (*v).to_string());
        }
        p
    }

    fn regex(pattern: &str) -> HostnameRegex {
        HostnameRegex::compile(pattern).unwrap().unwrap()
    }

    fn urls(spec: &[&str]) -> Vec<Url> {
        spec.iter().map(|u| Url::parse(u).unwrap()).collect()
    }

    fn text(urls: &[Url]) -> Vec<String> {
        urls.iter().map(|u| u.as_str().trim_end_matches('/').to_string()).collect()
    }

    fn candidates_for(p: &HashMap<String, String>, rank: usize) -> Option<Vec<String>> {
        extract_rank_candidate_urls(p)[rank].as_deref().map(text)
    }

    fn select(rank_urls: Option<&str>, pattern: Option<&str>) -> Result<Vec<Option<String>>, KineticaError> {
        let mut p = props(&[]);
        if let Some(v) = rank_urls {
            p.insert(SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS.to_string(), v.to_string());
        }
        let regex = pattern.map(regex);
        Ok(extract_rank_urls(&p, regex.as_ref())?
            .into_iter()
            .map(|u| u.map(|u| u.as_str().trim_end_matches('/').to_string()))
            .collect())
    }

    // ---- Splitting lists ----

    #[test]
    fn splitting_drops_trailing_empty_fields() {
        assert_eq!(split_dropping_trailing_empty("a;b;;", ";"), ["a", "b"]);
        assert_eq!(split_dropping_trailing_empty("a;;b", ";"), ["a", "", "b"]);
        assert_eq!(split_dropping_trailing_empty(";;", ";"), Vec::<&str>::new());
        assert_eq!(split_dropping_trailing_empty("", ";"), [""]);
        assert_eq!(split_dropping_trailing_empty("a", ";"), ["a"]);
        assert_eq!(split_dropping_trailing_empty("http://", "://"), ["http"]);
    }

    // ---- The order of a rank's candidate addresses ----

    #[test]
    fn a_configured_public_address_leads_and_the_enumerated_one_follows_it() {
        let p = props(&[
            ("conf.worker_http_server_urls", "http://127.0.0.1:9191"),
            ("conf.worker_http_server_urls_private", "http://127.0.0.1:9191"),
            ("conf.worker_http_server_ips", "172.17.0.2"),
            ("conf.worker_http_server_ports", "9191"),
            ("conf.use_https", "FALSE"),
        ]);
        assert_eq!(candidates_for(&p, 0).unwrap(), ["http://127.0.0.1:9191", "http://172.17.0.2:9191"]);
    }

    #[test]
    fn a_proxied_cluster_yields_three_addresses_advertised_first_direct_last() {
        let p = props(&[
            ("conf.worker_http_server_urls", "https://44.222.79.53:8082/gpudb-0"),
            ("conf.worker_http_server_urls_private", "https://10.223.0.12:8082/gpudb-0"),
            ("conf.worker_http_server_ips", "10.223.0.12"),
            ("conf.worker_http_server_ports", "9191"),
            ("conf.enable_httpd_proxy", "TRUE"),
            ("conf.httpd_proxy_use_https", "TRUE"),
            ("conf.use_https", "FALSE"),
        ]);
        assert_eq!(
            candidates_for(&p, 0).unwrap(),
            ["https://44.222.79.53:8082/gpudb-0", "https://10.223.0.12:8082/gpudb-0", "http://10.223.0.12:9191"]
        );
    }

    #[test]
    fn with_no_public_address_configured_the_private_address_leads_the_enumeration() {
        let p = props(&[
            ("conf.worker_http_server_urls", "http://10.1.0.5:9191,http://10.2.0.5:9191"),
            ("conf.worker_http_server_urls_private", "http://myhost:9191"),
            ("conf.worker_http_server_ips", "10.1.0.5,10.2.0.5"),
            ("conf.worker_http_server_ports", "9191"),
            ("conf.use_https", "FALSE"),
        ]);
        assert_eq!(
            candidates_for(&p, 0).unwrap(),
            ["http://myhost:9191", "http://10.1.0.5:9191", "http://10.2.0.5:9191"]
        );
    }

    #[test]
    fn an_address_reached_through_two_properties_appears_once() {
        let p = props(&[
            ("conf.worker_http_server_urls", "http://10.1.0.5:9191"),
            ("conf.worker_http_server_urls_private", "http://10.1.0.5:9191"),
            ("conf.worker_http_server_ips", "10.1.0.5"),
            ("conf.worker_http_server_ports", "9191"),
            ("conf.use_https", "FALSE"),
        ]);
        assert_eq!(candidates_for(&p, 0).unwrap(), ["http://10.1.0.5:9191"]);
    }

    #[test]
    fn the_built_address_takes_https_exactly_when_the_ranks_own_listener_does() {
        let p = props(&[
            ("conf.worker_http_server_urls", "http://public:9191"),
            ("conf.worker_http_server_ips", "10.1.0.5"),
            ("conf.worker_http_server_ports", "9191"),
            ("conf.use_https", "TRUE"),
        ]);
        assert!(candidates_for(&p, 0).unwrap().contains(&"https://10.1.0.5:9191".to_string()));
    }

    #[test]
    fn a_removed_rank_keeps_its_slot_across_all_four_properties() {
        let p = props(&[
            ("conf.worker_http_server_urls", "http://h0:9191;;http://h2:9193"),
            ("conf.worker_http_server_urls_private", "http://p0:9191;;http://p2:9193"),
            ("conf.worker_http_server_ips", "10.0.0.1;;10.0.0.3"),
            ("conf.worker_http_server_ports", "9191;;9193"),
            ("conf.use_https", "FALSE"),
        ]);
        let all = extract_rank_candidate_urls(&p);
        assert_eq!(all.len(), 3);
        assert!(all[1].is_none(), "the vacated slot must still be a placeholder");
        let rank2 = candidates_for(&p, 2).unwrap();
        assert!(rank2.contains(&"http://h2:9193".to_string()));
        assert!(rank2.contains(&"http://10.0.0.3:9193".to_string()));
    }

    #[test]
    fn an_ipv6_literal_is_skipped_rather_than_emitted_unbracketed() {
        let p = props(&[
            ("conf.worker_http_server_urls", "http://public:9191"),
            ("conf.worker_http_server_ips", "fe80::1,10.1.0.5"),
            ("conf.worker_http_server_ports", "9191"),
            ("conf.use_https", "FALSE"),
        ]);
        assert_eq!(candidates_for(&p, 0).unwrap(), ["http://public:9191", "http://10.1.0.5:9191"]);
    }

    #[test]
    fn an_older_server_sending_only_the_advertised_list_still_resolves() {
        let p = props(&[("conf.worker_http_server_urls", "http://h0:9191")]);
        assert_eq!(candidates_for(&p, 0).unwrap(), ["http://h0:9191"]);
    }

    #[test]
    fn ranks_sharing_a_host_are_grouped_together() {
        let p = props(&[
            ("conf.number_of_hosts", "2"),
            ("conf.host0_address", "10.223.0.12"),
            ("conf.host1_address", "10.223.0.252"),
            ("conf.rank0_ip_address", "10.223.0.12"),
            ("conf.rank1_ip_address", "10.223.0.12"),
            ("conf.rank2_ip_address", "10.223.0.252"),
        ]);
        assert_eq!(extract_rank_host_mapping(&p, 3), [Some(0), Some(0), Some(1)]);
    }

    #[test]
    fn a_removed_rank_is_grouped_with_no_host() {
        let p = props(&[
            ("conf.number_of_hosts", "1"),
            ("conf.host0_address", "10.0.0.1"),
            ("conf.rank0_ip_address", "10.0.0.1"),
            ("conf.rank1_ip_address", ""),
            ("conf.rank2_ip_address", "10.0.0.1"),
        ]);
        assert_eq!(extract_rank_host_mapping(&p, 3), [Some(0), None, Some(0)]);
    }

    #[test]
    fn the_join_does_not_fall_back_to_the_enumerated_addresses() {
        let p = props(&[
            ("conf.number_of_hosts", "1"),
            ("conf.host0_address", "127.0.0.1"),
            ("conf.rank0_ip_address", "172.17.0.2"),
            ("conf.worker_http_server_ips", "172.17.0.2"),
        ]);
        assert_eq!(extract_rank_host_mapping(&p, 1), [None]);
    }

    #[test]
    fn a_candidate_on_the_hosts_proven_address_is_tried_first() {
        let c = urls(&["https://public.example:8082/gpudb-2", "https://10.0.0.7:8082/gpudb-2", "http://10.0.0.7:9193"]);
        assert_eq!(
            text(&order_urls_by_preferred_host_first(&c, Some("10.0.0.7"))),
            ["https://10.0.0.7:8082/gpudb-2", "http://10.0.0.7:9193", "https://public.example:8082/gpudb-2"]
        );
    }

    #[test]
    fn the_preference_matches_on_host_alone() {
        let c = urls(&["https://other.example:8082/gpudb-5", "https://10.0.0.7:9999/gpudb-5"]);
        assert_eq!(text(&order_urls_by_preferred_host_first(&c, Some("10.0.0.7")))[0], "https://10.0.0.7:9999/gpudb-5");
    }

    #[test]
    fn without_a_matching_proven_address_the_order_is_untouched() {
        let c = urls(&["http://a:1", "http://b:2"]);
        assert_eq!(text(&order_urls_by_preferred_host_first(&c, None)), ["http://a:1", "http://b:2"]);
        assert_eq!(text(&order_urls_by_preferred_host_first(&c, Some("c"))), ["http://a:1", "http://b:2"]);
    }

    #[test]
    fn a_bridge_address_advertised_under_several_hosts_is_flagged() {
        let c: RankCandidates = vec![
            Some(urls(&["http://172.31.33.30:9191", "http://172.17.0.1:9191"])),
            Some(urls(&["http://172.31.33.30:9192", "http://172.17.0.1:9192"])),
            Some(urls(&["http://172.31.33.31:9193", "http://172.17.0.1:9193"])),
            Some(urls(&["http://172.31.33.32:9194"])),
        ];
        assert_eq!(
            non_identifying_hosts(&c, &[Some(0), Some(0), Some(1), Some(2)]),
            HashSet::from(["172.17.0.1".to_string()])
        );
    }

    #[test]
    fn an_address_shared_on_one_host_or_by_an_ungrouped_rank_still_identifies() {
        let c: RankCandidates = vec![Some(urls(&["http://10.0.0.7:9191"])), Some(urls(&["http://10.0.0.7:9192"]))];
        assert!(non_identifying_hosts(&c, &[Some(0), Some(0)]).is_empty());
        assert!(non_identifying_hosts(&c, &[Some(0), None]).is_empty());
        let with_hole: RankCandidates = vec![Some(urls(&["http://10.0.0.7:9191"])), None, Some(urls(&["http://10.0.0.8:9193"]))];
        assert!(non_identifying_hosts(&with_hole, &[Some(0), None, Some(1)]).is_empty());
    }

    #[test]
    fn an_ambiguous_address_is_tried_last_but_never_dropped() {
        let bridge = HashSet::from(["172.17.0.1".to_string()]);
        assert_eq!(
            text(&order_urls_by_unique_hosts_first(urls(&["http://172.17.0.1:9191", "http://172.31.33.30:9191"]), &bridge)),
            ["http://172.31.33.30:9191", "http://172.17.0.1:9191"]
        );
        assert_eq!(
            text(&order_urls_by_unique_hosts_first(urls(&["http://172.17.0.1:9191"]), &bridge)),
            ["http://172.17.0.1:9191"]
        );
        assert_eq!(
            text(&order_urls_by_unique_hosts_first(urls(&["http://a:1", "http://b:2"]), &HashSet::new())),
            ["http://a:1", "http://b:2"]
        );
    }

    // ---- Choosing each rank's URL ----

    #[test]
    fn one_url_per_rank_in_slot_order() {
        assert_eq!(
            select(Some("http://h0:9191;http://h1:9192;http://h2:9193"), None).unwrap(),
            [Some("http://h0:9191".into()), Some("http://h1:9192".into()), Some("http://h2:9193".into())]
        );
    }

    #[test]
    fn a_removed_rank_keeps_its_slot_and_a_removed_trailing_rank_compacts_away() {
        assert_eq!(
            select(Some("http://h0:9191;;http://h2:9193"), None).unwrap(),
            [Some("http://h0:9191".into()), None, Some("http://h2:9193".into())]
        );
        assert_eq!(
            select(Some("http://h0:9191;http://h1:9192;"), None).unwrap(),
            [Some("http://h0:9191".into()), Some("http://h1:9192".into())]
        );
    }

    #[test]
    fn unusable_alternates_are_skipped_and_the_first_usable_is_taken() {
        assert_eq!(
            select(Some("http://a:9191,http://b:9191;http://c:9192,http://d:9192"), None).unwrap(),
            [Some("http://a:9191".into()), Some("http://c:9192".into())]
        );
        assert_eq!(select(Some("notaurl,http://good:9191"), None).unwrap(), [Some("http://good:9191".into())]);
        assert_eq!(select(Some("http://:9191,http://good:9191"), None).unwrap(), [Some("http://good:9191".into())]);
        assert_eq!(select(Some("http://good:9191,,http://other:9191"), None).unwrap(), [Some("http://good:9191".into())]);
    }

    // Each pattern against each candidate: look-around, back-references,
    // atomic groups, possessive quantifiers, class intersections and \Q...\E
    // quotation included.
    #[test]
    fn the_regex_reads_the_full_pattern_syntax() {
        for (pattern, candidate, matches) in [
            (r"(?!10\.)\d+\.\d+", "10.0.0.1", false),
            (r"(?!10\.)\d+\.\d+", "172.17.0.2", true),
            (r"(\d+)\.\1\.", "10.10.0.1", true),
            (r"(\d+)\.\1\.", "10.11.0.1", false),
            (r"(?<=1)7", "172.17.0.2", false),
            (r"172\.(?=17)", "172.17.0.2", true),
            (r"172\.(?=18)", "172.17.0.2", false),
            (r"(?>a+)b", "aaab", true),
            (r"\d++\.", "172.17", true),
            (r"[a-z&&[^e]]+", "host", true),
            (r"[a-z&&[^e]]+", "edge", false),
            (r"\Q10.0\E", "10.0.0.1", true),
            (r"\Q10.0\E", "1000", false),
            (r"172\.17\.", "172.17.0.2", true),
            (r"10\.0\.0\.1$", "10.0.0.10", false),
        ] {
            assert_eq!(regex(pattern).matches(candidate), matches, "{pattern} on {candidate}");
        }
        assert_eq!(literal_quotes_escaped(r"a\Q.*\Eb\\Qc\Q+"), r"a\.\*b\\Qc\+");
    }

    #[test]
    fn the_regex_selects_by_prefix_on_the_host_alone() {
        assert_eq!(
            select(Some("http://192.168.1.5:9191,http://10.0.0.5:9191"), Some(r"10\.0\.")).unwrap(),
            [Some("http://10.0.0.5:9191".into())]
        );
        assert_eq!(select(Some("http://10.0.0.10:9191"), Some(r"10\.0\.0\.1")).unwrap(), [Some("http://10.0.0.10:9191".into())]);
        assert!(matches!(select(Some("http://10.0.0.5:9191"), Some("9191")), Err(KineticaError::HostnameRegexFailure(_))));
    }

    #[test]
    fn the_regex_is_blamed_only_when_it_rejected_a_usable_address() {
        assert!(matches!(
            select(Some("http://10.0.0.5:9191"), Some(r"192\.168\.")),
            Err(KineticaError::HostnameRegexFailure(_))
        ));
        assert!(matches!(select(Some("http://:9191"), Some(r"10\.0\.")), Err(KineticaError::Api(_))));
        assert!(matches!(select(Some("http://:9191"), None), Err(KineticaError::Api(_))));
    }

    #[test]
    fn nothing_to_resolve_yields_an_empty_list() {
        assert!(select(None, None).unwrap().is_empty());
        assert!(select(Some(""), None).unwrap().is_empty());
        let mut p = props(&[("conf.worker_http_server_urls", "http://h0:9191")]);
        p.insert(SYSTEM_PROPERTIES_RESPONSE_ENABLE_MH.to_string(), "FALSE".to_string());
        assert!(extract_rank_urls(&p, None).unwrap().is_empty());
    }

    #[test]
    fn caller_urls_are_accepted_only_inside_the_patterns_space() {
        let accepted = |pattern: Option<&str>, spec: &[&str]| {
            let regex = pattern.and_then(|p| HostnameRegex::compile(p).unwrap());
            text(&urls_accepted_by_hostname_regex(&urls(spec), regex.as_ref()))
        };
        assert_eq!(accepted(Some(r"172\.17\.1\."), &["http://172.17.1.5:9191"]), ["http://172.17.1.5:9191"]);
        assert!(accepted(Some(r"172\.17\.1\."), &["http://10.0.0.5:9191"]).is_empty());
        assert_eq!(
            accepted(Some(r"172\.17\.1\."), &["http://172.17.1.5:9191", "http://10.0.0.5:9191", "http://172.17.1.6:9191"]),
            ["http://172.17.1.5:9191", "http://172.17.1.6:9191"]
        );
        assert_eq!(accepted(None, &["http://10.0.0.5:9191"]), ["http://10.0.0.5:9191"]);
        assert_eq!(accepted(Some(""), &["http://10.0.0.5:9191"]), ["http://10.0.0.5:9191"]);
    }

    // ---- Host names and the HA ring ----

    fn host_props(hosts: &[&str]) -> HashMap<String, String> {
        let mut p = HashMap::from([(SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS.to_string(), hosts.len().to_string())]);
        for (i, h) in hosts.iter().enumerate() {
            p.insert(format!("conf.host{i}_public_urls"), (*h).to_string());
        }
        p
    }

    #[test]
    fn host_names_are_matched_on_the_host_but_kept_with_port_and_path() {
        let p = host_props(&["http://10.0.0.5:9191/x,http://172.17.0.2:9191"]);
        assert_eq!(host_names_from_system_properties(&p, None).unwrap(), HashSet::from(["10.0.0.5:9191/x".to_string()]));
        let r = regex(r"172\.");
        assert_eq!(host_names_from_system_properties(&p, Some(&r)).unwrap(), HashSet::from(["172.17.0.2:9191".to_string()]));
    }

    #[test]
    fn a_host_is_blamed_on_the_regex_only_when_it_rejected_a_usable_entry() {
        let r = regex(r"192\.");
        let rejected = host_names_from_system_properties(&host_props(&["http://10.0.0.5"]), Some(&r)).unwrap_err();
        assert_eq!(
            rejected.to_string(),
            "Hostname regex failure: No matching hostname found for host #0 (given hostname regex 192\\.)"
        );
        let nothing_usable = host_names_from_system_properties(&host_props(&["http://:9191"]), Some(&r)).unwrap_err();
        assert!(matches!(nothing_usable, KineticaError::Api(ref m) if m == "No matching hostname found for host #0."));
    }

    #[test]
    fn the_head_node_urls_of_the_ring_follow_the_same_rules() {
        let mut p = HashMap::from([
            (SYSTEM_PROPERTIES_RESPONSE_ENABLE_HA.to_string(), "true".to_string()),
            (SYSTEM_PROPERTIES_RESPONSE_HEAD_NODE_URLS.to_string(), "notaurl,http://a:9191;http://b:9191,http://c:9191;".to_string()),
        ]);
        assert_eq!(text(&ha_ring_head_node_urls(&p, None).unwrap()), ["http://a:9191", "http://b:9191"]);
        let r = regex("c");
        assert!(matches!(ha_ring_head_node_urls(&p, Some(&r)), Err(KineticaError::HostnameRegexFailure(_))));
        p.insert(SYSTEM_PROPERTIES_RESPONSE_ENABLE_HA.to_string(), "FALSE".to_string());
        assert!(ha_ring_head_node_urls(&p, None).unwrap().is_empty());
    }

    #[test]
    fn the_host_manager_path_replaces_the_last_segment() {
        let path = |u: &str| host_manager_path_for(&Url::parse(u).unwrap());
        assert_eq!(path("http://h:9191"), "/gpudb-host-manager");
        assert_eq!(path("https://h:8082/gpudb-0"), "/gpudb-host-manager");
        assert_eq!(path("https://h:8082/prefix/gpudb-0"), "/prefix/gpudb-host-manager");
        assert_eq!(path("https://h:8082/prefix/gpudb-0/"), "/prefix/gpudb-0/gpudb-host-manager");
    }

    #[test]
    fn an_invalid_pattern_is_reported_with_its_error() {
        let err = HostnameRegex::compile("(").unwrap_err().to_string();
        assert!(err.starts_with("API error: Error in parsing the pattern for hostname regex; given: '('; got error: "), "{err}");
        assert!(HostnameRegex::compile("").unwrap().is_none());
    }

    // ==================== Credibility of advertised addresses ====================
    // The routable addresses are from TEST-NET-3, which no machine running
    // these tests carries.

    fn rank_slots(specs: &[Option<&str>]) -> Vec<Option<Url>> {
        specs.iter().map(|spec| spec.map(|s| Url::parse(s).unwrap())).collect()
    }

    /// Rank 0 loopback and ranks 1 and 2 routable, the shape that would shift
    /// every rank down if compacted: withdrawn whole.
    #[tokio::test]
    async fn a_mixed_rank_list_is_withdrawn_whole_rather_than_compacted() {
        let advertised = rank_slots(&[
            Some("http://127.0.0.1:9191"),
            Some("http://203.0.113.5:9192"),
            Some("http://203.0.113.6:9193"),
        ]);
        assert!(rank_urls_if_all_credible(advertised, false, "rank address").await.is_empty());
    }

    #[tokio::test]
    async fn a_wholly_routable_rank_list_passes_through_with_its_removed_slots() {
        let advertised = rank_slots(&[Some("http://203.0.113.4:9191"), None, Some("http://203.0.113.6:9193")]);
        assert_eq!(rank_urls_if_all_credible(advertised.clone(), false, "rank address").await, advertised);
    }

    /// The configuration every test against a local instance runs.
    #[tokio::test]
    async fn a_server_on_this_machine_keeps_its_loopback_addresses() {
        let advertised = rank_slots(&[
            Some("http://127.0.0.1:9191"),
            Some("http://127.0.0.1:9192"),
            Some("http://127.0.0.1:9193"),
        ]);
        assert_eq!(rank_urls_if_all_credible(advertised.clone(), true, "rank address").await, advertised);
    }

    /// Alternatives for one rank, so removing one reorders nothing.
    #[tokio::test]
    async fn one_ranks_alternatives_are_filtered_one_by_one() {
        let candidates = vec![Url::parse("http://127.0.0.1:9191").unwrap(), Url::parse("http://203.0.113.5:9191").unwrap()];
        assert_eq!(
            retain_credible_urls(Some(candidates.clone()), false, "candidate").await,
            Some(vec![Url::parse("http://203.0.113.5:9191").unwrap()])
        );
        assert_eq!(retain_credible_urls(Some(candidates.clone()), true, "candidate").await, Some(candidates));
        assert_eq!(retain_credible_urls(None, false, "candidate").await, None);
    }

    #[tokio::test]
    async fn an_address_is_local_if_loopback_or_on_an_interface_here() {
        for host in ["127.0.0.1", "127.9.9.9", "localhost", "[::1]"] {
            assert!(is_local_address(host).await, "{host}");
        }
        // Whatever this machine's interfaces carry, link-local included
        for interface in if_addrs::get_if_addrs().unwrap() {
            let ip = interface.ip();
            let host = if ip.is_ipv6() { format!("[{ip}]") } else { ip.to_string() };
            assert!(is_local_address(&host).await, "{host} is on interface {}", interface.name);
        }
        for host in ["203.0.113.7", "", "no-such-host.invalid"] {
            assert!(!is_local_address(host).await, "{host:?}");
        }
    }

    // ==================== Cluster identity ====================

    #[test]
    fn a_cluster_signature_is_its_names_and_rank_urls() {
        let properties = props(&[
            (SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS_PRIVATE, " http://10.0.0.1:9191 "),
            (SYSTEM_PROPERTIES_RESPONSE_RING_NAME, "ring"),
            (SYSTEM_PROPERTIES_RESPONSE_CLUSTER_NAME, "  "),
            (SYSTEM_PROPERTIES_RESPONSE_SERVER_URLS, "http://h:9191;http://h:9192"),
            (SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS, "1"),
        ]);
        assert_eq!(
            cluster_identity_signature(&properties),
            "conf.ring_name=ring\nconf.worker_http_server_urls=http://h:9191;http://h:9192\n\
             conf.worker_http_server_urls_private=http://10.0.0.1:9191\n"
        );
        assert_eq!(cluster_identity_signature(&HashMap::new()), "");
    }

    // ==================== Published host manager address ====================

    #[test]
    fn the_published_host_manager_url_is_the_one_for_the_head_nodes_host() {
        let head = Url::parse("http://h2:9191").unwrap();
        let properties = props(&[
            (SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS, " 3 "),
            ("conf.host0_host_manager_public_url", "http://h1:9300"),
            ("conf.host1_host_manager_public_url", "not a url"),
            ("conf.host2_host_manager_public_url", " https://h2:9443/hm "),
        ]);
        assert_eq!(
            published_host_manager_url(&properties, &head),
            Some(Url::parse("https://h2:9443/hm").unwrap()),
            "matched by host, the port aside"
        );

        // Only as many hosts as the cluster reports are looked at
        let mut fewer = properties.clone();
        fewer.insert(SYSTEM_PROPERTIES_RESPONSE_NUM_HOSTS.to_string(), "2".to_string());
        assert_eq!(published_host_manager_url(&fewer, &head), None);

        // Set for other hosts only, or not at all
        assert_eq!(published_host_manager_url(&properties, &Url::parse("http://h3:9191").unwrap()), None);
        assert_eq!(published_host_manager_url(&props(&[]), &head), None);
    }

    #[test]
    fn the_published_host_manager_port_falls_back_when_absent_or_not_a_number() {
        assert_eq!(published_host_manager_port(&props(&[(SYSTEM_PROPERTIES_RESPONSE_HM_PORT, " 9555 ")]), 9300), 9555);
        assert_eq!(published_host_manager_port(&props(&[(SYSTEM_PROPERTIES_RESPONSE_HM_PORT, "nine")]), 9300), 9300);
        assert_eq!(published_host_manager_port(&props(&[]), 9301), 9301);
    }
}

