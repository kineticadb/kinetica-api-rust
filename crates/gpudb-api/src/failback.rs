//! Fail-back to the primary cluster after an HA failover.
//!
//! When a failover moves a [`Kinetica`](crate::Kinetica) connection off the
//! primary cluster, a poller starts checking the primary every
//! [`KineticaOptions::failback_polling_interval`](crate::KineticaOptions::failback_polling_interval)
//! seconds. Once the primary passes three checks, the connection switches
//! back to it.
//!
//! The poller runs as a task of its own and holds no reference to the
//! connection, only to its cluster ring (see [`FailbackTarget`]), through which
//! it switches the connection back itself, as soon as the primary is ready.

use crate::generic_record::RecordValue;
use crate::gpudb::{ClusterAddressInfo, HeaderState};
use crate::hyper_client::HyperClient;
use crate::{Kinetica, KineticaOptions, ShowSystemStatusRequest, SqlIterator};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use url::Url;

const HA_STATUS_KEY: &str = "ha_status";
const HA_STATUS_DRAINED_KEY: &str = "drained";
const HA_STATUS_DRAINED_VALUE_DRAINING: &str = "draining";

/// A running fail-back poller. Dropping it stops the poll.
pub(crate) struct FailbackPoller {
    task: tokio::task::JoinHandle<()>,
}

/// What the poller switches the connection back to the primary with: the
/// connection's cluster ring, shared with it.
pub(crate) struct FailbackTarget {
    pub(crate) host_addresses: Arc<RwLock<Vec<ClusterAddressInfo>>>,
    pub(crate) ha_url_indices: Arc<RwLock<Vec<usize>>>,
    pub(crate) current_cluster_index_pointer: Arc<RwLock<usize>>,
    pub(crate) switch_lock: Arc<tokio::sync::Mutex<()>>,
}

impl FailbackTarget {
    /// Switches the connection back to the primary cluster, which the poller
    /// does as soon as the primary is ready.
    ///
    /// Under the switch lock, so the move cannot interleave with a failover's
    /// own index arithmetic, the pointer goes to index 0, where the ring keeps
    /// the primary, and the primary owes a multi-head probe, as an arrival by
    /// failover does. The probe owed is made by the next multi-head object to
    /// ask for the cluster's addresses (see
    /// `Kinetica::acquire_multi_head_snapshot`), every one of which asks
    /// before routing.
    ///
    /// It does **not** touch the cluster-switch count: that count is the
    /// baseline for `switch_url`'s per-caller arithmetic, and bumping it here
    /// would spend one of every in-flight caller's attempts, so a cluster
    /// coming back could make another caller conclude the ring was exhausted.
    pub(crate) async fn fail_back(&self) {
        {
            let _switching = self.switch_lock.lock().await;
            *self.current_cluster_index_pointer.write() = 0;
            let slot = crate::gpudb::cluster_slot(&self.host_addresses.read(), &self.ha_url_indices.read(), 0);
            if let Some(slot) = slot {
                if let Some(cluster) = self.host_addresses.write().get_mut(slot) {
                    cluster.set_probe_owed(true);
                }
            }
        }
        info!("Failed back to the primary cluster; re-establishing multi-head");
    }
}

/// What the poller takes from the connection to check the primary with: its
/// HTTP client and headers for the ping, and its credentials for a connection
/// to the primary.
pub(crate) struct PrimaryAccess {
    pub(crate) client: HyperClient,
    pub(crate) headers: Arc<HeaderState>,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) bypass_ssl_cert_check: bool,
    /// The connection's trust store
    pub(crate) trust_store_file_path: String,
    pub(crate) trust_store_password: String,
}

impl FailbackPoller {
    /// Starts polling `primary`, switching `target` back to it once it can
    /// take requests, and stopping then.
    pub(crate) fn start(primary: Url, access: PrimaryAccess, polling_interval: Duration, target: FailbackTarget) -> Self {
        debug!("Starting the fail-back poller for primary cluster {primary}");
        let task = tokio::spawn(async move {
            loop {
                debug!("Polling primary cluster {primary} for fail-back...");
                if poll(&access, &primary).await {
                    info!("Failback to primary cluster at [{primary}] succeeded");
                    target.fail_back().await;
                    debug!("Poll successful. Stopping the poller...");
                    return;
                }
                tokio::time::sleep(polling_interval).await;
            }
        });
        Self { task }
    }

    /// Whether the poll is still going on.
    pub(crate) fn is_running(&self) -> bool {
        !self.task.is_finished()
    }
}

impl Drop for FailbackPoller {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Whether the primary can take requests again.
///
/// 1. Its head node answers the liveness ping.
/// 2. A connection to it alone (no failover, no auto-discovery) reports HA
///    queues that are not still draining.
/// 3. It runs `SELECT <now>` and returns that value.
///
/// A primary that reports no `ha_status` at all fails the second check, so
/// the connection does not fail back to it.
async fn poll(access: &PrimaryAccess, primary: &Url) -> bool {
    // The liveness ping goes through the connection, with its headers.
    if !crate::gpudb::is_kinetica_running_at(&access.client, &access.headers, primary).await {
        debug!("Kinetica is not running at URL: [{primary}]");
        return false;
    }

    let mut options = KineticaOptions::default();
    options
        .set_disable_auto_discovery(true)
        .set_disable_failover(true)
        .set_username(&access.username)
        .set_password(&access.password)
        .set_bypass_ssl_cert_check(access.bypass_ssl_cert_check)
        .set_trust_store_file_path(&access.trust_store_file_path)
        .set_trust_store_password(&access.trust_store_password);
    let db = match Kinetica::new(vec![primary.to_string()], options).await {
        Ok(db) => Arc::new(db),
        Err(e) => {
            debug!("Could not connect to Kinetica: {e}");
            return false;
        }
    };

    let status = db
        .show_system_status_raw(&ShowSystemStatusRequest { options: HashMap::new() })
        .await
        .ok()
        .and_then(|r| r.status_map.get(HA_STATUS_KEY).cloned());
    let drained = status
        .as_deref()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|v| v.get(HA_STATUS_DRAINED_KEY).and_then(|d| d.as_str()).map(str::to_string));
    match drained.as_deref() {
        Some(HA_STATUS_DRAINED_VALUE_DRAINING) => {
            debug!("Kinetica running on primary cluster at [{primary}], but HA queues are still draining");
            return false;
        }
        Some(_) => {}
        None => {
            warn!("Could not parse system status block [{status:?}] for URL: [{primary}]");
            return false;
        }
    }

    let time_check = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default();
    let mut records = match SqlIterator::new(db, &format!("SELECT {time_check}")).await {
        Ok(records) => records,
        Err(e) => {
            warn!("Kinetica running on primary cluster at [{primary}], but query check failed: [{e}]");
            return false;
        }
    };
    let running = if records.total_records() != 1 {
        debug!(
            "Kinetica running on primary cluster at [{primary}], but query check failed to return \
             expected result count--instead [{}]",
            records.total_records()
        );
        false
    } else {
        match records.next().await {
            Ok(record) => match record.get(0) {
                Some(RecordValue::Long(value)) if *value == time_check => true,
                Some(RecordValue::Long(value)) => {
                    debug!(
                        "Kinetica running on primary cluster at [{primary}], but query check failed to \
                         return expected result--instead [{value}] != [{time_check}]"
                    );
                    false
                }
                other => {
                    warn!(
                        "Kinetica running on primary cluster at [{primary}], but query check failed: \
                         [not an i64: {other:?}]"
                    );
                    false
                }
            },
            Err(e) => {
                warn!("Kinetica running on primary cluster at [{primary}], but query check failed: [{e}]");
                false
            }
        }
    };
    records.close().await;
    running
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// A URL that refuses connections while the returned socket lives.
    fn closed_url() -> (socket2::Socket, Url) {
        let (held, address) = crate::test_net::refusing_address();
        (held, Url::parse(&format!("http://{address}/")).unwrap())
    }

    /// A server that answers every request with `body`.
    async fn answering(body: &'static str) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let _ = stream.read(&mut buf).await;
                    let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                    let _ = stream.write_all(resp.as_bytes()).await;
                });
            }
        });
        url
    }

    fn access() -> PrimaryAccess {
        PrimaryAccess {
            client: HyperClient::new(),
            headers: Arc::new(HeaderState::default()),
            username: "u".into(),
            password: "p".into(),
            bypass_ssl_cert_check: false,
            trust_store_file_path: String::new(),
            trust_store_password: String::new(),
        }
    }

    #[tokio::test]
    async fn a_primary_that_does_not_answer_is_not_ready() {
        let (_held, url) = closed_url();
        assert!(!poll(&access(), &url).await);
    }

    #[tokio::test]
    async fn a_primary_that_is_not_kinetica_is_not_ready() {
        assert!(!poll(&access(), &answering("hello").await).await);
    }

    #[tokio::test]
    async fn the_running_check_wants_kinetica_s_exact_answer() {
        let client = HyperClient::new();
        let headers = HeaderState::default();
        let (kinetica, almost) = (answering("Kinetica is running!").await, answering("Kinetica is running!!").await);
        assert!(crate::gpudb::is_kinetica_running_at(&client, &headers, &kinetica).await);
        assert!(!crate::gpudb::is_kinetica_running_at(&client, &headers, &almost).await);
    }

    #[tokio::test]
    async fn the_poller_keeps_polling_until_the_primary_is_ready() {
        let pointer = Arc::new(RwLock::new(1));
        let target = FailbackTarget {
            host_addresses: Default::default(),
            ha_url_indices: Default::default(),
            current_cluster_index_pointer: pointer.clone(),
            switch_lock: Default::default(),
        };
        let (_held, url) = closed_url();
        let poller = FailbackPoller::start(url, access(), Duration::from_millis(50), target);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(poller.is_running());
        assert_eq!(*pointer.read(), 1, "not switched back");
    }
}
