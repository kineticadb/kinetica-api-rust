//! Common utilities for examples
//!
//! This module provides helper functions used across all examples.

use gpudb_api::KineticaOptions;
use std::env;

/// The server URL: `KINETICA_URL`, else `KI_URL`, or none where neither is
/// set.
pub fn kinetica_url() -> Option<String> {
    env_first(&["KINETICA_URL", "KI_URL"])
}

/// The username: `KINETICA_USER`, else `KI_USER`, else `admin`.
pub fn kinetica_user() -> String {
    env_first(&["KINETICA_USER", "KI_USER"]).unwrap_or_else(|| "admin".to_string())
}

/// The password: `KINETICA_PASSWORD`, else `KI_PWD`, else `password`.
pub fn kinetica_password() -> String {
    env_first(&["KINETICA_PASSWORD", "KI_PWD"]).unwrap_or_else(|| "password".to_string())
}

/// The value of the first of the environment variables `names` that is set
/// and not empty.
fn env_first(names: &[&str]) -> Option<String> {
    first_set(names.iter().map(|name| env::var(name).ok()))
}

/// The first value that is present and not empty.
fn first_set(values: impl IntoIterator<Item = Option<String>>) -> Option<String> {
    values.into_iter().flatten().find(|value| !value.is_empty())
}

/// Loads Kinetica connection configuration from environment variables.
///
/// # Environment Variables
///
/// - `KINETICA_URL`, else `KI_URL`: Server URL (default: "http://localhost:9191")
/// - `KINETICA_USER`, else `KI_USER`: Username (default: "admin")
/// - `KINETICA_PASSWORD`, else `KI_PWD`: Password (default: "password")
/// - `KINETICA_USE_SNAPPY`: Enable Snappy compression (default: "false", set to "true" to enable)
///
/// An empty variable counts as unset.
///
/// # Returns
///
/// A tuple of (urls, options) where urls is a Vec<String> and options is KineticaOptions.
pub fn connection_config() -> (Vec<String>, KineticaOptions) {
    let url = kinetica_url().unwrap_or_else(|| "http://localhost:9191".to_string());
    let username = kinetica_user();
    let password = kinetica_password();

    let use_snappy = env::var("KINETICA_USE_SNAPPY")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);

    let mut options = KineticaOptions::new();
    options.set_username(username)
           .set_password(password)
           // On-prem deployments commonly present a self-signed certificate;
           // set KINETICA_BYPASS_SSL=1 to accept it.
           .set_bypass_ssl_cert_check(
               std::env::var("KINETICA_BYPASS_SSL").as_deref() == Ok("1"),
           )
           .set_use_snappy(use_snappy);

    (vec![url], options)
}

#[cfg(test)]
mod tests {
    use super::first_set;

    // The first variable set wins; an empty one counts as unset.
    #[test]
    fn the_first_variable_set_and_not_empty_is_taken() {
        let some = |s: &str| Some(s.to_string());
        assert_eq!(first_set([some("a"), some("b")]).as_deref(), Some("a"));
        assert_eq!(first_set([None, some("b")]).as_deref(), Some("b"));
        assert_eq!(first_set([some(""), some("b")]).as_deref(), Some("b"));
        assert_eq!(first_set([None, some("")]), None);
    }
}
