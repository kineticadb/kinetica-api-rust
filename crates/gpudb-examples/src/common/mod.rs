//! Common utilities for examples
//!
//! This module provides helper functions used across all examples.

use gpudb_api::KineticaOptions;
use std::env;

/// Loads Kinetica connection configuration from environment variables.
///
/// # Environment Variables
///
/// - `KINETICA_URL`: Server URL (default: "http://localhost:9191")
/// - `KINETICA_USER`: Username (default: "admin")
/// - `KINETICA_PASSWORD`: Password (default: "password")
/// - `KINETICA_USE_SNAPPY`: Enable Snappy compression (default: "false", set to "true" to enable)
///
/// # Returns
///
/// A tuple of (urls, options) where urls is a Vec<String> and options is KineticaOptions.
pub fn connection_config() -> (Vec<String>, KineticaOptions) {
    let url = env::var("KINETICA_URL")
        .unwrap_or_else(|_| "http://localhost:9191".to_string());

    let username = env::var("KINETICA_USER")
        .unwrap_or_else(|_| "admin".to_string());

    let password = env::var("KINETICA_PASSWORD")
        .unwrap_or_else(|_| "password".to_string());

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
