//! Waiting, in tests, for what another task does.

use std::time::Duration;

/// Whether `done` comes to hold within five seconds, checked every 10 ms.
///
/// For asserting that something happens, which a fixed sleep can only
/// guess the time of; what must *not* happen is still checked after a sleep.
pub(crate) async fn eventually(done: impl Fn() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if done() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
