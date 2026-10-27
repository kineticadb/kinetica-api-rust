//! Records what the client reports of itself: the compiler version, which it
//! reports in its `User-Agent` header, and the API version.
//!
//! The API version is the crate's own version, `MAJOR.MINOR.REVISION-ABI`,
//! reported as `MAJOR.MINOR.REVISION.ABI`: `7.2.3-0` is `7.2.3.0`. The build
//! number is the time of the build, `yyyyMMddHHmmss`.
//!
//! The repository's `VERSION` file is the source of the crate version: the
//! schema-processing script in gpudb-generators writes it into `Cargo.toml`,
//! as Cargo versions have only three numbers and take the ABI version as a
//! pre-release.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    // "rustc 1.88.0 (6b00bc388 2025-06-23)" -> "1.88.0"
    let version = Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| text.split_whitespace().nth(1).map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GPUDB_RUSTC_VERSION={version}");
    println!("cargo:rerun-if-env-changed=RUSTC");

    // MAJOR.MINOR.REVISION.ABI, from the crate version's MAJOR.MINOR.REVISION
    // and its pre-release, the ABI version
    let part = |name: &str| std::env::var(format!("CARGO_PKG_VERSION_{name}")).unwrap_or_default();
    let abi = part("PRE");
    if abi.is_empty() || !abi.bytes().all(|b| b.is_ascii_digit()) {
        panic!(
            "the crate version {} is not MAJOR.MINOR.REVISION-ABI, as the schema-processing \
             script writes it from VERSION",
            std::env::var("CARGO_PKG_VERSION").unwrap_or_default()
        );
    }
    let api_version = format!(
        "{}.{}.{}.{abi}",
        part("MAJOR"),
        part("MINOR"),
        part("PATCH")
    );
    println!("cargo:rustc-env=GPUDB_API_VERSION={api_version}");
    println!(
        "cargo:rustc-env=GPUDB_API_VERSION_FULL={api_version}-{}",
        build_timestamp()
    );
    // The version is in the manifest; a new one stamps a new build number
    println!("cargo:rerun-if-changed=Cargo.toml");
}

/// The time of the build, `yyyyMMddHHmmss`, in UTC.
fn build_timestamp() -> String {
    let seconds = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()) as i64;
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
