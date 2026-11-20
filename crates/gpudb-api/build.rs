//! Records what the client reports of itself: the compiler version, which it
//! reports in its `User-Agent` header, and the API version.
//!
//! The API version is the crate's own SemVer version, `MAJOR.MINOR.PATCH`:
//! `MAJOR.MINOR` is the Kinetica version the crate implements, and `PATCH`
//! counts the crate's own releases of it. The build number is the time of the
//! build, `yyyyMMddHHmmss`.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    println!("cargo:rustc-env=GPUDB_RUSTC_VERSION={}", rustc_version());
    println!("cargo:rerun-if-env-changed=RUSTC");

    let api_version = release_version();
    let build = build_number();

    println!("cargo:rustc-env=GPUDB_API_VERSION={api_version}");
    println!("cargo:rustc-env=GPUDB_API_BUILD_NUMBER={build}");
    // The version is in the manifest; a new one stamps a new build number
    println!("cargo:rerun-if-changed=Cargo.toml");
}

/// The version of the compiler building the crate, `1.88.0` from
/// `rustc 1.88.0 (6b00bc388 2025-06-23)`, or `unknown` if it cannot be read.
fn rustc_version() -> String {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| text.split_whitespace().nth(1).map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// The release version, the crate's `MAJOR.MINOR.PATCH`.
///
/// Panics if the crate version is a pre-release (`7.2.3-1`): releases are
/// plain SemVer, so one would be a manifest written in another scheme.
fn release_version() -> String {
    let part = |name: &str| std::env::var(format!("CARGO_PKG_VERSION_{name}")).unwrap_or_default();

    if !part("PRE").is_empty() {
        panic!(
            "the crate version {} is a pre-release; release versions are MAJOR.MINOR.PATCH",
            std::env::var("CARGO_PKG_VERSION").unwrap_or_default()
        );
    }

    format!("{}.{}.{}", part("MAJOR"), part("MINOR"), part("PATCH"))
}

/// The build number: the time of the build, `yyyyMMddHHmmss`, in UTC.
fn build_number() -> String {
    let seconds = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()) as i64;
    utc_timestamp(seconds)
}

/// The time `seconds` after the epoch, `yyyyMMddHHmmss`, in UTC.
fn utc_timestamp(seconds: i64) -> String {
    let (days, time) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}{:02}{:02}{:02}",
        time / 3600,
        time % 3600 / 60,
        time % 60
    )
}
