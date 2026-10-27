//! Worker list for multi-head ingest and retrieval.
//!
//! This module provides the [`WorkerList`] struct, a view onto the worker rank
//! addresses a [`Kinetica`] connection has already discovered.

use crate::gpudb::MultiHeadSnapshot;
use crate::Kinetica;
use log::debug;
use std::sync::Arc;
use url::Url;

/// A list of worker URLs to use for multi-head operations.
///
/// The addresses belong to the **connection**, not to this list. A [`Kinetica`]
/// instance discovers each cluster's rank addresses, applies the connection's
/// hostname regex to them, and keeps the result per cluster; this struct is a
/// view onto that, held by a bulk inserter or record retriever.
///
/// It does not resolve or filter addresses of its own. A filter has no coherent
/// meaning here, because it cannot survive a failover: one tuned for a cluster's
/// addressing may match nothing on the next, and multi-head objects fail over
/// with the connection. Set the regex on the connection instead — see
/// `KineticaOptions::set_hostname_regex` — where it is applied to every cluster
/// as that cluster is discovered.
///
/// The one choice that *is* meaningful per object is declining multi-head
/// altogether; see [`WorkerList::empty`]. That names an intent rather than an
/// address, and an intent transfers across clusters where a list of addresses
/// does not.
///
/// Note entry `i` of this list is rank `i + 1`; the head rank is not a member,
/// and a rank removed from the cluster keeps its slot as `None`. The worker
/// indices in the shard routing table are rank numbers, so compacting the list
/// would silently misroute every rank above the hole.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::{Kinetica, KineticaOptions, WorkerList};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let options = KineticaOptions::new();
/// let kinetica = Arc::new(
///     Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
/// );
///
/// // Use the addresses the connection discovered for its current cluster
/// let worker_list = WorkerList::from_cluster(&kinetica);
///
/// // Or decline multi-head for one inserter/retriever, keeping fail-over
/// let head_node_only = WorkerList::empty();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct WorkerList {
    /// List of worker URLs. `None` entries represent removed ranks.
    urls: Vec<Option<Url>>,

    /// Whether the caller asked for head-node-only operation by handing over an
    /// empty list.
    ///
    /// Named for the contract rather than the origin: a list built from real
    /// addresses is also supplied by a caller, and does *not* mean this.
    disable_multi_head: bool,
}

/// The same as [`WorkerList::empty`], which declines multi-head operations.
/// Internal code must not use it, as `empty` says.
impl Default for WorkerList {
    fn default() -> Self {
        Self::empty()
    }
}

impl WorkerList {
    /// Creates an empty [`WorkerList`], which declines multi-head operations for
    /// the bulk inserter or record retriever it is given to.
    ///
    /// That object will route through the head node for its lifetime. The choice
    /// is authoritative: it is not undone when the worker list is rebuilt after
    /// a failover or a shard rebalance.
    ///
    /// This is the way to decline multi-head for a single object while keeping
    /// the connection's fail-over. Disabling auto-discovery is not equivalent —
    /// it also stops the client discovering the other clusters in an HA ring, so
    /// it costs fail-over as well.
    ///
    /// Nothing inside this crate may use this constructor: it is a caller's way
    /// of expressing an intent, and an internal use would silently pin its own
    /// list to head-node-only. Internal code wanting a list with no addresses
    /// builds one through [`WorkerList::with_urls`] instead.
    pub fn empty() -> Self {
        Self {
            urls: Vec::new(),
            disable_multi_head: true,
        }
    }

    /// A list of the given worker rank URLs, with `None` in the slot of any
    /// rank removed from the cluster.
    pub(crate) fn with_urls(urls: Vec<Option<Url>>) -> Self {
        Self {
            urls,
            disable_multi_head: false,
        }
    }

    /// Creates a [`WorkerList`] from the worker rank addresses the given
    /// connection has already discovered for the cluster it is currently on.
    ///
    /// This reads the connection's addresses; it does not query the server or
    /// re-resolve them. They were filtered through the connection's hostname
    /// regex when that cluster was discovered, so they already reflect it.
    ///
    /// If the connection has no worker addresses for the current cluster —
    /// multi-head is off at the server, or the cluster advertises none this
    /// client can use — the list is empty and the object using it operates
    /// through the head node.
    ///
    /// # Arguments
    ///
    /// * `db` - The Kinetica instance whose addresses to use
    pub fn from_cluster(db: &Arc<Kinetica>) -> Self {
        Self::from_snapshot(db.current_multi_head_snapshot().as_deref())
    }

    /// Creates a [`WorkerList`] from a multi-head snapshot the caller already
    /// holds.
    ///
    /// A caller rebuilding its worker list has already taken a snapshot and
    /// must build from that same one; reading the connection again would let
    /// another probe land in between, so the object would build from one answer
    /// and record having seen another.
    ///
    /// No snapshot, or one whose cluster cannot give this client multi-head,
    /// yields an **empty** list, which makes the object holding it operate
    /// through the head node. An empty list is not a declined one: a later
    /// rebuild can still populate it. Only [`WorkerList::empty`] declines.
    pub(crate) fn from_snapshot(snapshot: Option<&MultiHeadSnapshot>) -> Self {
        match snapshot {
            Some(snapshot) if snapshot.is_multi_head_available() => {
                Self::with_urls(snapshot.worker_rank_urls().to_vec())
            }
            _ => {
                debug!("Multi-head is not available on the current cluster; worker list is empty");
                Self::with_urls(Vec::new())
            }
        }
    }

    /// Whether multi-head operations can be used with this list.
    ///
    /// # Returns
    ///
    /// Whether the list names any worker rank.
    pub fn is_multi_head_enabled(&self) -> bool {
        !self.is_empty()
    }

    /// Whether this list expresses a request for head-node-only operation.
    ///
    /// True only for a list built with [`WorkerList::empty`], the empty list a
    /// caller hands over to turn multi-head off for one bulk inserter or record
    /// retriever. Such a list is authoritative: it is never replaced, including
    /// when the worker list is rebuilt after a failover or a shard rebalance.
    ///
    /// **Used internally,** this is the routing flag the rebuild paths consult.
    pub(crate) fn disables_multi_head(&self) -> bool {
        self.disable_multi_head
    }

    /// Returns the number of workers in the list.
    pub fn len(&self) -> usize {
        self.urls.len()
    }

    /// Returns `true` if the worker list is empty.
    pub fn is_empty(&self) -> bool {
        self.urls.is_empty()
    }

    /// Gets the URL at the specified index.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the worker URL to get (0-based, corresponds to rank-1)
    ///
    /// # Returns
    ///
    /// The URL at the specified index, or `None` if the rank was removed or the
    /// index is out of bounds.
    pub fn get(&self, index: usize) -> Option<&Url> {
        self.urls.get(index).and_then(|opt| opt.as_ref())
    }

    /// Returns a reference to the underlying vector of URLs.
    ///
    /// `None` entries represent removed worker ranks.
    pub fn urls(&self) -> &Vec<Option<Url>> {
        &self.urls
    }

    /// Returns an iterator over the URLs in the worker list.
    ///
    /// This iterator yields references to `Option<Url>`, where `None` represents
    /// removed worker ranks.
    pub fn iter(&self) -> impl Iterator<Item = &Option<Url>> {
        self.urls.iter()
    }

    /// Adds a URL to the worker list.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to add, or `None` to add a placeholder for a removed rank
    pub fn push(&mut self, url: Option<Url>) {
        self.urls.push(url);
    }

    /// Adds a URL to the worker list (convenience method that wraps in `Some`).
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to add
    pub fn add(&mut self, url: Url) {
        self.urls.push(Some(url));
    }

    /// Converts the worker list to a vector of URL strings.
    ///
    /// This method filters out `None` entries (removed ranks) and returns only
    /// the valid URL strings.
    ///
    /// # Never use this for shard routing
    ///
    /// The result is **compacted**, so its indices are no longer rank numbers.
    /// Routing a sharded record through these indices sends every rank above a
    /// removed one to the wrong host. Use [`Self::urls`] and index by slot, or
    /// let the bulk inserter build its rank mapping from the slot positions.
    /// This is only safe where any live worker will do — round-robin
    /// distribution, health checks, and the like.
    ///
    /// # Returns
    ///
    /// A vector of URL strings for all valid (non-removed) workers.
    pub fn to_url_strings(&self) -> Vec<String> {
        self.urls
            .iter()
            .filter_map(|opt| opt.as_ref().map(|url| url.to_string()))
            .collect()
    }

    /// Consumes the worker list and returns a vector of URL strings.
    ///
    /// This method filters out `None` entries (removed ranks) and returns only
    /// the valid URL strings.
    ///
    /// # Never use this for shard routing
    ///
    /// The result is compacted; see [`Self::to_url_strings`] for why that makes
    /// it unusable for routing sharded records.
    ///
    /// # Returns
    ///
    /// A vector of URL strings for all valid (non-removed) workers.
    pub fn into_url_strings(self) -> Vec<String> {
        self.urls
            .into_iter()
            .filter_map(|opt| opt.map(|url| url.to_string()))
            .collect()
    }
}

impl PartialEq for WorkerList {
    /// Two lists are equal when they name the same ranks, holes included.
    ///
    /// The head-node-only intent is deliberately excluded: this compares
    /// addresses, and it is what the rebuild paths use to decide whether a
    /// cluster's addresses have actually changed.
    fn eq(&self, other: &Self) -> bool {
        self.urls == other.urls
    }
}

impl Eq for WorkerList {}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn the_default_list_declines_multi_head() {
        let worker_list = WorkerList::default();
        assert!(worker_list.is_empty());
        assert!(worker_list.disables_multi_head());
    }

    #[test]
    fn an_empty_list_declines_multi_head() {
        let worker_list = WorkerList::empty();
        assert!(worker_list.is_empty());
        assert!(!worker_list.is_multi_head_enabled());
        assert!(
            worker_list.disables_multi_head(),
            "an empty list is an authoritative request for head-node-only"
        );
    }

    /// The intent is what distinguishes the two, not the emptiness: a list that
    /// merely happens to have no addresses must not pin its owner to the head
    /// node forever.
    #[test]
    fn a_list_that_happens_to_be_empty_does_not_decline_multi_head() {
        let worker_list = WorkerList::with_urls(Vec::new());
        assert!(worker_list.is_empty());
        assert!(!worker_list.disables_multi_head());
    }

    #[test]
    fn from_urls_keeps_removed_rank_slots() {
        let worker_list = WorkerList::with_urls(vec![
            Some(url("http://worker1:9191")),
            None,
            Some(url("http://worker3:9191")),
        ]);

        assert_eq!(worker_list.len(), 3);
        assert_eq!(worker_list.get(0).unwrap().as_str(), "http://worker1:9191/");
        assert!(worker_list.get(1).is_none(), "removed rank keeps its slot");
        assert_eq!(worker_list.get(2).unwrap().as_str(), "http://worker3:9191/");
        assert!(!worker_list.disables_multi_head());
    }

    #[test]
    fn add_and_push() {
        let mut worker_list = WorkerList::with_urls(Vec::new());
        worker_list.add(url("http://worker1:9191"));
        worker_list.push(None); // Removed rank
        worker_list.add(url("http://worker3:9191"));

        assert_eq!(worker_list.len(), 3);
        assert!(worker_list.get(0).is_some());
        assert!(worker_list.get(1).is_none());
        assert!(worker_list.get(2).is_some());
    }

    #[test]
    fn compacted_strings_drop_the_holes() {
        let worker_list = WorkerList::with_urls(vec![
            Some(url("http://worker1:9191")),
            None,
            Some(url("http://worker3:9191")),
        ]);

        assert_eq!(worker_list.to_url_strings().len(), 2);
    }

    #[test]
    fn equality_compares_addresses_including_holes() {
        let a = WorkerList::with_urls(vec![Some(url("http://w1:9191")), None]);
        let b = WorkerList::with_urls(vec![Some(url("http://w1:9191")), None]);
        let c = WorkerList::with_urls(vec![Some(url("http://w1:9191"))]);

        assert_eq!(a, b);
        assert_ne!(a, c, "a hole is part of the identity of the list");
    }

    /// The rebuild paths compare lists to decide whether anything changed, so
    /// the head-node-only intent must not make an otherwise identical list
    /// compare unequal.
    #[test]
    fn equality_ignores_the_head_node_only_intent() {
        assert_eq!(WorkerList::empty(), WorkerList::with_urls(Vec::new()));
    }
}
