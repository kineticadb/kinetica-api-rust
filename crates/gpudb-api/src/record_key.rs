use std::io::Cursor;
use std::sync::LazyLock;
use murmur3::murmur3_x64_128;
use regex::Regex;
use chrono::{Datelike, Timelike, NaiveDate, Months};

use crate::errors::KineticaError;
use crate::record_key_builder::is_unsigned_long;

/// The lowest year the server can encode into a shard key.
const MIN_SUPPORTED_YEAR: i32 = 1000;

/// The highest year the server can encode into a shard key.
const MAX_SUPPORTED_YEAR: i32 = 2900;

// The patterns spell out ASCII digit classes, where the regex crate's `\d` is
// any Unicode digit: a value in other digits does not match, and is no key.
// `^...$` takes the whole text, a trailing newline included.

/// Date: `YYYY-MM-DD`
static DATE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})$").unwrap()
});

/// IPv4: a dotted quad of one to three digits each
static IPV4_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$").unwrap()
});

/// Datetime: `YYYY-MM-DD H:MM:SS[.mmm]`, one space between date and time, a
/// one- or two-digit hour, and exactly three digits of milliseconds
static DATETIME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{1,2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]{3}))?$").unwrap()
});

/// Time: `H:MM:SS[.mmm]`, a one- or two-digit hour, and exactly three digits
/// of milliseconds
static TIME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([0-9]{1,2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]{3}))?$").unwrap()
});

/// The number a regex group of at most four ASCII digits spells.
fn digits(group: &str) -> i32 {
    group.bytes().fold(0, |n, digit| n * 10 + i32::from(digit - b'0'))
}

/// Pre-compiled regex for UUID parsing (8-4-4-4-12 hex format).
///
/// The separators are optional and the trailing group of 12 is captured as
/// 4 + 8 — the split is how the final group is packed, and the optional
/// dashes are why a dashless UUID is accepted rather than reported as
/// unparseable.
static UUID_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([0-9a-fA-F]{8})-?([0-9a-fA-F]{4})-?([0-9a-fA-F]{4})-?([0-9a-fA-F]{4})-?([0-9a-fA-F]{4})([0-9a-fA-F]{8})$").unwrap()
});

/// Maps every encoding of a floating point value to one representative:
/// `-0.0` becomes `+0.0` and every NaN becomes one quiet NaN.
///
/// This matches what the server does when it builds a shard key. The key's
/// bytes are hashed to pick a shard, so two values that compare equal have to
/// produce the same bytes or they are routed to different shards. Infinities
/// have a single encoding each and are returned unchanged.
///
/// `write_f64` preserves whatever bit pattern it is handed, and arithmetic can
/// produce NaNs whose payload differs from the canonical one. The canonical
/// NaN is [`f64::NAN`] (`0x7ff8000000000000`).
fn canonicalize_f64_key(value: f64) -> f64 {
    if value == 0.0 {
        // Catches -0.0 and +0.0, returns the positive one
        0.0
    } else if value.is_nan() {
        f64::NAN
    } else {
        value
    }
}

/// The `f32` counterpart of [`canonicalize_f64_key`].
///
/// The canonical NaN is [`f32::NAN`] (`0x7fc00000`).
fn canonicalize_f32_key(value: f32) -> f32 {
    if value == 0.0 {
        0.0
    } else if value.is_nan() {
        f32::NAN
    } else {
        value
    }
}

/// Builds a date leniently: from January 1st of `year`, advanced by
/// `month - 1` months and `day - 1` days, so out-of-range components roll
/// over instead of failing.
///
/// A date is parsed with a regex that admits any two-digit month and day, so
/// `2020-02-30` is accepted and encodes with March 1st's day-of-year and
/// day-of-week.
// Rolling over, rather than failing, keeps such a value routing to the same
// shard as other Kinetica clients route it.
///
/// Returns `None` only when the resulting date falls outside what a
/// [`NaiveDate`] can represent at all.
fn lenient_ymd(year: i32, month: i32, day: i32) -> Option<NaiveDate> {
    let base = NaiveDate::from_ymd_opt(year, 1, 1)?;

    let month_offset = month - 1;
    let shifted = if month_offset >= 0 {
        base.checked_add_months(Months::new(month_offset as u32))?
    } else {
        base.checked_sub_months(Months::new(month_offset.unsigned_abs()))?
    };

    shifted.checked_add_signed(chrono::Duration::days(i64::from(day) - 1))
}

#[derive(Debug)]
pub struct RecordKey {
    /// The encoded key bytes.
    ///
    /// Private because the invariants that make a key routable are maintained
    /// by the `add_*` methods: the buffer must end up exactly
    /// `RecordKeyBuilder::buffer_size` long, and `routing_hash` must be the
    /// murmur hash of precisely these bytes. External mutation breaks both
    /// silently, and the symptom is records landing on the wrong shard.
    buffer: Vec<u8>,
    routing_hash: u64,
    is_valid: bool,
    /// Why this key is unusable, when the reason is worth reporting.
    ///
    /// Distinct from `is_valid`, which is set for every unusable value
    /// including the deliberate placeholders an ingest must tolerate. A value
    /// only earns a reason here when a caller supplied it expressly to be
    /// looked up, where nothing will substitute a usable value for it. See
    /// [`RecordKey::mark_invalid`].
    invalid_reason: Option<String>,
}

impl RecordKey {
    pub fn new(size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(size),
            routing_hash: 0,
            is_valid: true,
            invalid_reason: None,
        }
    }

    /// The encoded key bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// The hash that selects the shard, valid once
    /// [`RecordKey::compute_hashes`] has run.
    pub fn routing_hash(&self) -> u64 {
        self.routing_hash
    }

    /// Whether every value encoded into this key was usable.
    pub fn is_valid(&self) -> bool {
        self.is_valid
    }

    /// Takes the reason this key is unusable, if one was worth reporting.
    ///
    /// Consuming rather than borrowing: the look-up path turns the reason into
    /// an error and there is no second reader.
    pub(crate) fn take_invalid_reason(&mut self) -> Option<String> {
        self.invalid_reason.take()
    }

    /// Flags the key as unusable, recording why.
    ///
    /// When a record is being *ingested*, the value may be a deliberate
    /// placeholder for a column that initializes itself server-side
    /// (`init_with_now`, `init_with_uuid`), in which case there is nothing to
    /// report and nothing to route on — the server assigns the real value and
    /// shards on that. When a key is being *looked up*, on the other hand, the
    /// caller supplied the value directly and an unparseable one is simply an
    /// error. Recording the reason here lets the lookup path report it.
    fn mark_invalid(&mut self, reason: String) {
        self.is_valid = false;

        // Keep the first reason; it is the one closest to the actual problem
        if self.invalid_reason.is_none() {
            self.invalid_reason = Some(reason);
        }
    }

    pub fn route(&self, routing_table: &[i32]) -> usize {
        if routing_table.is_empty() { return 0; }
        let hash = self.routing_hash as i64;
        let index = (hash % (routing_table.len() as i64)).unsigned_abs() as usize;
        (routing_table[index] - 1) as usize
    }

    pub fn compute_hashes(&mut self) {
        // Murmur3 0.5.2 returns Result<u128> and takes 2 arguments
        let h128 = murmur3_x64_128(&mut Cursor::new(&self.buffer), 10).unwrap();
        self.routing_hash = (h128 & 0xFFFFFFFFFFFFFFFF) as u64;
    }

    pub fn push_i32(&mut self, v: i32) { self.buffer.extend_from_slice(&v.to_le_bytes()); }
    pub fn push_i64(&mut self, v: i64) { self.buffer.extend_from_slice(&v.to_le_bytes()); }

    pub fn push_f32(&mut self, v: f32) {
        self.buffer.extend_from_slice(&canonicalize_f32_key(v).to_le_bytes());
    }

    pub fn push_f64(&mut self, v: f64) {
        self.buffer.extend_from_slice(&canonicalize_f64_key(v).to_le_bytes());
    }

    /// Appends a null value of a type whose encoding is `width` bytes wide:
    /// that many zero bytes.
    pub fn push_null(&mut self, width: usize) {
        self.buffer.resize(self.buffer.len() + width, 0);
    }

    /// Appends a boolean value (1 byte: 1 for true, 0 for false).
    pub fn push_bool(&mut self, v: bool) {
        self.buffer.push(if v { 1 } else { 0 });
    }

    /// Appends an 8-bit signed integer (1 byte).
    pub fn push_i8(&mut self, v: i8) {
        self.buffer.push(v as u8);
    }

    /// Appends a 16-bit signed integer (2 bytes, little-endian).
    pub fn push_i16(&mut self, v: i16) {
        self.buffer.extend_from_slice(&v.to_le_bytes());
    }

    pub fn push_char(&mut self, val: &str, length: usize) {
        let bytes = val.as_bytes();
        let len = bytes.len().min(length);
        for _ in 0..(length - len) { self.buffer.push(0); }
        for i in (0..len).rev() { self.buffer.push(bytes[i]); } // Match Go's reversed packing
    }

    pub fn push_string(&mut self, val: &str) {
        let h128 = murmur3_x64_128(&mut Cursor::new(val.as_bytes()), 10).unwrap();
        self.push_i64((h128 & 0xFFFFFFFFFFFFFFFF) as i64);
    }

    pub fn push_date(&mut self, val: &str) {
        let Some(c) = DATE_REGEX.captures(val) else {
            self.push_i32(0);
            self.is_valid = false;
            return;
        };

        let (y, m, d) = (digits(&c[1]), digits(&c[2]), digits(&c[3]));

        // A year the server cannot encode. This is also what a column with the
        // 'init_with_now' property carries, so the server can substitute its
        // own value at insertion time.
        if !(MIN_SUPPORTED_YEAR..=MAX_SUPPORTED_YEAR).contains(&y) {
            self.push_i32(0);
            self.is_valid = false;
            return;
        }

        // Lenient; see lenient_ymd(). Note the packed month and day are the
        // values as given, not the rolled ones — only the day-of-year and
        // day-of-week come from the resolved date.
        let Some(dt) = lenient_ymd(y, m, d) else {
            self.push_i32(0);
            self.is_valid = false;
            return;
        };

        // Parentheses fix bit-shift generic ambiguity
        let packed: i32 = ((y - 1900) << 21) | (m << 17) | (d << 12) |
            ((dt.ordinal() as i32) << 3) | (dt.weekday().number_from_sunday() as i32);
        self.push_i32(packed);
    }

    /// Appends a timestamp value (milliseconds since Unix epoch) as 8 bytes packed datetime.
    ///
    /// Encoding (64 bits):
    /// - Bits 53-63: year-1900 (11 bits)
    /// - Bits 49-52: month (4 bits)
    /// - Bits 44-48: day (5 bits)
    /// - Bits 39-43: hour (5 bits)
    /// - Bits 33-38: minute (6 bits)
    /// - Bits 27-32: second (6 bits)
    /// - Bits 17-26: millisecond (10 bits)
    /// - Bits 8-16: day of year (9 bits)
    /// - Bits 5-7: day of week (3 bits)
    pub fn push_timestamp(&mut self, millis: i64) {
        use chrono::{DateTime, Utc};
        let Some(dt) = DateTime::from_timestamp_millis(millis) else {
            self.push_i64(0);
            self.is_valid = false;
            return;
        };

        let dt: DateTime<Utc> = dt;

        // Anything outside the supported year range is an invalid timestamp,
        // which is exactly what a column with the 'init_with_now' property is
        // given to make the server substitute NOW() at insertion time. Bail out
        // here the same way push_date() does for an out-of-range date.
        //
        // chrono reports an astronomical year — negative for BC — so the
        // comparison is direct: the init_with_now sentinel of -99990224000000
        // falls in 1200 BC, which a check of the year *within the era* would
        // let through, to encode identically to 1200 AD.
        let y = i64::from(dt.year());
        if !(i64::from(MIN_SUPPORTED_YEAR)..=i64::from(MAX_SUPPORTED_YEAR)).contains(&y) {
            self.push_i64(0);
            self.is_valid = false;
            return;
        }

        let m = dt.month() as i64;
        let d = dt.day() as i64;
        let hour = dt.hour() as i64;
        let min = dt.minute() as i64;
        let sec = dt.second() as i64;
        // Taken from the resolved datetime rather than `millis % 1000`, which
        // is negative for a pre-epoch timestamp and would pack a different
        // value than the server's 0-999 millisecond field.
        let ms = i64::from(dt.timestamp_subsec_millis());
        let ordinal = dt.ordinal() as i64;
        let weekday = dt.weekday().number_from_sunday() as i64;

        let packed: i64 = ((y - 1900) << 53)
            | (m << 49)
            | (d << 44)
            | (hour << 39)
            | (min << 33)
            | (sec << 27)
            | (ms << 17)
            | (ordinal << 8)
            | (weekday << 5);
        self.push_i64(packed);
    }

    /// Appends a datetime value from string format "YYYY-MM-DD HH:MM:SS.mmm"
    pub fn push_datetime(&mut self, val: &str) {
        let Some(c) = DATETIME_REGEX.captures(val) else {
            self.push_i64(0);
            self.is_valid = false;
            return;
        };
        let [y, m, d, hour, min, sec] = [1, 2, 3, 4, 5, 6].map(|i| i64::from(digits(&c[i])));
        let ms = c.get(7).map_or(0, |ms| i64::from(digits(ms.as_str())));

        // A year the server cannot encode; see push_date().
        if !(i64::from(MIN_SUPPORTED_YEAR)..=i64::from(MAX_SUPPORTED_YEAR)).contains(&y) {
            self.push_i64(0);
            self.is_valid = false;
            return;
        }

        // No field's range is checked here. Each is packed as given, and the
        // day of the year and of the week are those of the date the time rolls
        // into: 24:00:00 is the next day's midnight. See lenient_ymd() for the
        // date itself.
        let Some(date) = lenient_ymd(y as i32, m as i32, d as i32) else {
            self.push_i64(0);
            self.is_valid = false;
            return;
        };
        let Some(rolled) = date
            .and_hms_opt(0, 0, 0)
            .and_then(|midnight| midnight.checked_add_signed(chrono::Duration::seconds(hour * 3600 + min * 60 + sec)))
            .map(|date_time| date_time.date())
        else {
            self.push_i64(0);
            self.is_valid = false;
            return;
        };
        let ordinal = i64::from(rolled.ordinal());
        let weekday = i64::from(rolled.weekday().number_from_sunday());

        let packed: i64 = ((y - 1900) << 53)
            | (m << 49)
            | (d << 44)
            | (hour << 39)
            | (min << 33)
            | (sec << 27)
            | (ms << 17)
            | (ordinal << 8)
            | (weekday << 5);
        self.push_i64(packed);
    }

    /// Appends a time value from string format "HH:MM:SS.mmm"
    ///
    /// Encoding (32 bits):
    /// - Bits 26-31: hour (6 bits)
    /// - Bits 20-25: minute (6 bits)
    /// - Bits 14-19: second (6 bits)
    /// - Bits 4-13: millisecond (10 bits)
    pub fn push_time(&mut self, val: &str) {
        let Some(c) = TIME_REGEX.captures(val) else {
            self.push_i32(0);
            self.is_valid = false;
            return;
        };
        let (hour, min, sec) = (digits(&c[1]), digits(&c[2]), digits(&c[3]));
        let ms = c.get(4).map_or(0, |ms| digits(ms.as_str()));

        // A time past 23:59:59 is no key
        if hour > 23 || min > 59 || sec > 59 {
            self.push_i32(0);
            self.is_valid = false;
            return;
        }

        let packed: i32 = (hour << 26) | (min << 20) | (sec << 14) | (ms << 4);
        self.push_i32(packed);
    }

    /// Appends a UUID value from string format "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    ///
    /// Encoding (16 bytes) - special little-endian packing:
    /// - Bytes 0-3: Last 8 hex digits of last group (reversed)
    /// - Bytes 4-5: First 4 hex digits of last group (reversed)
    /// - Bytes 6-7: Fourth group (reversed)
    /// - Bytes 8-9: Third group (reversed)
    /// - Bytes 10-11: Second group (reversed)
    /// - Bytes 12-15: First group (reversed)
    pub fn push_uuid(&mut self, val: &str) {
        if let Some(c) = UUID_REGEX.captures(val) {
            // Parse each group as hex.  The trailing 12 hex digits arrive
            // already split into groups 5 and 6.
            let group1 = u32::from_str_radix(&c[1], 16).unwrap_or(0); // 8 hex digits
            let group2 = u16::from_str_radix(&c[2], 16).unwrap_or(0); // 4 hex digits
            let group3 = u16::from_str_radix(&c[3], 16).unwrap_or(0); // 4 hex digits
            let group4 = u16::from_str_radix(&c[4], 16).unwrap_or(0); // 4 hex digits
            let group5_first = u16::from_str_radix(&c[5], 16).unwrap_or(0); // 4 hex digits
            let group5_last = u32::from_str_radix(&c[6], 16).unwrap_or(0);  // 8 hex digits

            // Write in the specific order (little-endian within each group)
            self.buffer.extend_from_slice(&group5_last.to_le_bytes());   // Bytes 0-3
            self.buffer.extend_from_slice(&group5_first.to_le_bytes());  // Bytes 4-5
            self.buffer.extend_from_slice(&group4.to_le_bytes());        // Bytes 6-7
            self.buffer.extend_from_slice(&group3.to_le_bytes());        // Bytes 8-9
            self.buffer.extend_from_slice(&group2.to_le_bytes());        // Bytes 10-11
            self.buffer.extend_from_slice(&group1.to_le_bytes());        // Bytes 12-15
        } else {
            // Invalid UUID - write 16 zero bytes
            for _ in 0..16 {
                self.buffer.push(0);
            }
            // Recorded with a reason, unlike the other unparseable values: a
            // malformed UUID is never a placeholder the server will fill in, so
            // a look-up given one can report it instead of silently returning
            // no records.
            self.mark_invalid(format!("Value '{val}' could not be parsed as a UUID!"));
        }
    }

    /// Appends an unsigned long given as text: the value's low 64 bits,
    /// little-endian like every other number in the key.
    ///
    /// The text must be one to twenty digits, or this writes nothing and
    /// fails. That check is of the form, not the range, so a twenty-digit
    /// value above `u64::MAX` passes and keeps only its low 64 bits.
    ///
    /// Only ASCII digits are accepted, as for a look-up filter (see
    /// `RecordKeyBuilder::build_expression`). The server refuses other digits
    /// too ("invalid ulong"), so such a record fails either way; here it fails
    /// before it is sent.
    pub fn push_ulong(&mut self, val: &str) -> Result<(), KineticaError> {
        if !is_unsigned_long(val) {
            return Err(KineticaError::Config(format!(
                "Value must be a positive integer less than or equal to the maximum unsigned long value, got {val}"
            )));
        }

        // Twenty digits at most, so the value fits a u128.
        let value = val.bytes().fold(0u128, |n, digit| n * 10 + u128::from(digit - b'0'));
        self.buffer.extend_from_slice(&(value as u64).to_le_bytes());
        Ok(())
    }

    pub fn push_ipv4(&mut self, val: &str) {
        let Some(octets) = IPV4_REGEX.captures(val) else {
            self.push_i32(0);
            self.is_valid = false;
            return;
        };
        let [a, b, c, d] = [1, 2, 3, 4].map(|i| digits(&octets[i]));

        // An octet over 255 is no address
        if a > 255 || b > 255 || c > 255 || d > 255 {
            self.push_i32(0);
            self.is_valid = false;
            return;
        }

        self.push_i32((a << 24) | (b << 16) | (c << 8) | d);
    }

    /// Appends a decimal value given as text: the text is parsed as an exact
    /// decimal and rounded HALF_UP to `scale`, and text that is not a number,
    /// or is wider than `precision` at that scale, encodes as zero. A
    /// precision of up to 18 takes 8 bytes and a wider one 12, both two's
    /// complement.
    pub fn push_decimal(&mut self, val: &str, precision: usize, scale: usize) {
        let unscaled = crate::big_decimal::parse_decimal_value(val, precision, scale)
            .unwrap_or_default();
        if precision <= 18 {
            let (low, _) = crate::big_decimal::low_bits(&unscaled, 64);
            self.push_i64(low as i64);
        } else {
            let (low, high) = crate::big_decimal::low_bits(&unscaled, 96);
            self.push_i64(low as i64);
            self.push_i32(high as i32);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encodes one value and hands back the resulting key, for byte comparison.
    fn key_with(f: impl FnOnce(&mut RecordKey)) -> RecordKey {
        let mut key = RecordKey::new(16);
        f(&mut key);
        key
    }

    // ==================== Floating point canonicalization ====================
    // Two values that compare equal must produce the same key bytes, or they
    // route to different shards than the server picked.

    #[test]
    fn negative_zero_hashes_as_positive_zero() {
        assert_eq!(
            key_with(|k| k.push_f64(-0.0)).buffer,
            key_with(|k| k.push_f64(0.0)).buffer
        );
        assert_eq!(
            key_with(|k| k.push_f32(-0.0f32)).buffer,
            key_with(|k| k.push_f32(0.0f32)).buffer
        );
    }

    #[test]
    fn every_nan_payload_collapses_to_one_encoding() {
        // A NaN with a non-canonical payload, the kind arithmetic can produce.
        let odd_nan = f64::from_bits(0x7ff8_0000_dead_beef);
        assert!(odd_nan.is_nan());
        assert_ne!(odd_nan.to_bits(), f64::NAN.to_bits());

        assert_eq!(
            key_with(|k| k.push_f64(odd_nan)).buffer,
            key_with(|k| k.push_f64(f64::NAN)).buffer
        );

        let odd_nan32 = f32::from_bits(0x7fc0_beef);
        assert!(odd_nan32.is_nan());
        assert_ne!(odd_nan32.to_bits(), f32::NAN.to_bits());

        assert_eq!(
            key_with(|k| k.push_f32(odd_nan32)).buffer,
            key_with(|k| k.push_f32(f32::NAN)).buffer
        );
    }

    #[test]
    fn canonical_nan_matches_the_server_constant() {
        // Pinned so a future std change cannot move the encoding out from
        // under the server silently.
        assert_eq!(f64::NAN.to_bits(), 0x7ff8_0000_0000_0000);
        assert_eq!(f32::NAN.to_bits(), 0x7fc0_0000);
    }

    #[test]
    fn ordinary_and_infinite_values_are_untouched() {
        assert_eq!(
            key_with(|k| k.push_f64(1.5)).buffer,
            1.5f64.to_le_bytes().to_vec()
        );
        assert_eq!(
            key_with(|k| k.push_f64(f64::INFINITY)).buffer,
            f64::INFINITY.to_le_bytes().to_vec()
        );
        assert_eq!(
            key_with(|k| k.push_f64(f64::NEG_INFINITY)).buffer,
            f64::NEG_INFINITY.to_le_bytes().to_vec()
        );
        // The two infinities stay distinct — only zeroes and NaNs collapse
        assert_ne!(
            key_with(|k| k.push_f64(f64::INFINITY)).buffer,
            key_with(|k| k.push_f64(f64::NEG_INFINITY)).buffer
        );
    }

    // ==================== Supported year range ====================

    #[test]
    fn timestamp_outside_the_supported_years_is_invalid() {
        // The init_with_now sentinel.  It lands in 1200 BC, which a range check
        // on an era-relative year would wave through and then encode as 1200 AD.
        let key = key_with(|k| k.push_timestamp(-99990224000000));
        assert!(!key.is_valid);
        assert_eq!(key.buffer, 0i64.to_le_bytes().to_vec());
    }

    #[test]
    fn a_bc_year_does_not_collide_with_the_ad_year_of_the_same_number() {
        // 1200 BC and 1200 AD must not produce the same bytes.  The BC value is
        // rejected outright, which is what keeps them apart.
        let bc = key_with(|k| k.push_timestamp(-99990224000000));
        let ad = key_with(|k| k.push_timestamp(-24298300800000)); // ~1200-03-01 AD
        assert!(!bc.is_valid);
        assert!(ad.is_valid);
        assert_ne!(bc.buffer, ad.buffer);
    }

    #[test]
    fn dates_outside_the_supported_years_are_invalid() {
        for bad in ["0999-12-31", "2901-01-01"] {
            let key = key_with(|k| k.push_date(bad));
            assert!(!key.is_valid, "{bad} should be rejected");
            assert_eq!(key.buffer, 0i32.to_le_bytes().to_vec());
        }

        // The boundaries themselves are in range
        for good in ["1000-01-01", "2900-12-31"] {
            assert!(key_with(|k| k.push_date(good)).is_valid, "{good} should be accepted");
        }
    }

    #[test]
    fn datetimes_outside_the_supported_years_are_invalid() {
        let key = key_with(|k| k.push_datetime("0999-12-31 23:59:59"));
        assert!(!key.is_valid);
        assert_eq!(key.buffer, 0i64.to_le_bytes().to_vec());

        assert!(key_with(|k| k.push_datetime("2020-06-15 12:30:45.123")).is_valid);
    }

    #[test]
    fn a_pre_epoch_timestamp_packs_a_positive_millisecond_field() {
        // Regression: `millis % 1000` is negative before 1970, which corrupted
        // every field above it in the packed value.
        let key = key_with(|k| k.push_timestamp(-86_399_500)); // 1969-12-31, .500
        assert!(key.is_valid);

        let packed = i64::from_le_bytes(key.buffer[..8].try_into().unwrap());
        let ms = (packed >> 17) & 0x3ff;
        assert_eq!(ms, 500);
    }

    // ==================== Lenient dates ====================

    #[test]
    fn an_out_of_range_day_rolls_over_rather_than_panicking() {
        // The date is built leniently: the month and day pack as given, while
        // the day-of-year and day-of-week come from the resolved date of
        // 2020-03-01.
        let key = key_with(|k| k.push_date("2020-02-30"));
        assert!(key.is_valid);

        let packed = i32::from_le_bytes(key.buffer[..4].try_into().unwrap());
        assert_eq!((packed >> 21) & 0x7ff, 2020 - 1900);
        assert_eq!((packed >> 17) & 0xf, 2, "month packs as given");
        assert_eq!((packed >> 12) & 0x1f, 30, "day packs as given");

        let march_first = NaiveDate::from_ymd_opt(2020, 3, 1).unwrap();
        assert_eq!((packed >> 3) & 0x1ff, march_first.ordinal() as i32);
        assert_eq!(packed & 0x7, march_first.weekday().number_from_sunday() as i32);
    }

    #[test]
    fn a_well_formed_date_is_unaffected_by_leniency() {
        let key = key_with(|k| k.push_date("2020-06-15"));
        assert!(key.is_valid);

        let packed = i32::from_le_bytes(key.buffer[..4].try_into().unwrap());
        let d = NaiveDate::from_ymd_opt(2020, 6, 15).unwrap();
        let expected: i32 = ((2020 - 1900) << 21)
            | (6 << 17)
            | (15 << 12)
            | ((d.ordinal() as i32) << 3)
            | (d.weekday().number_from_sunday() as i32);
        assert_eq!(packed, expected);
    }

    // ==================== UUIDs ====================

    #[test]
    fn a_dashless_uuid_encodes_the_same_as_a_dashed_one() {
        let dashed = key_with(|k| k.push_uuid("123e4567-e89b-12d3-a456-426614174000"));
        let dashless = key_with(|k| k.push_uuid("123e4567e89b12d3a456426614174000"));

        assert!(dashed.is_valid);
        assert!(dashless.is_valid, "a dashless UUID is accepted");
        assert_eq!(dashed.buffer, dashless.buffer);
        assert_eq!(dashed.buffer.len(), 16);
    }

    #[test]
    fn uuid_group_packing_is_unchanged() {
        let key = key_with(|k| k.push_uuid("123e4567-e89b-12d3-a456-426614174000"));

        let mut expected = Vec::new();
        expected.extend_from_slice(&0x14174000u32.to_le_bytes()); // last 8 hex of final group
        expected.extend_from_slice(&0x4266u16.to_le_bytes()); // first 4 hex of final group
        expected.extend_from_slice(&0xa456u16.to_le_bytes()); // fourth group
        expected.extend_from_slice(&0x12d3u16.to_le_bytes()); // third group
        expected.extend_from_slice(&0xe89bu16.to_le_bytes()); // second group
        expected.extend_from_slice(&0x123e4567u32.to_le_bytes()); // first group

        assert_eq!(key.buffer, expected);
    }

    #[test]
    fn a_malformed_uuid_records_why() {
        let key = key_with(|k| k.push_uuid("not-a-uuid"));
        assert!(!key.is_valid);
        assert_eq!(
            key.invalid_reason.as_deref(),
            Some("Value 'not-a-uuid' could not be parsed as a UUID!")
        );
        assert_eq!(key.buffer, vec![0u8; 16]);
    }

    #[test]
    fn the_first_reason_is_the_one_kept() {
        let mut key = RecordKey::new(32);
        key.push_uuid("first-bad");
        key.push_uuid("second-bad");
        assert_eq!(
            key.invalid_reason.as_deref(),
            Some("Value 'first-bad' could not be parsed as a UUID!")
        );
    }

    #[test]
    fn an_unparseable_non_uuid_value_carries_no_reason() {
        // Only a UUID earns a reason; an out-of-range date may be a deliberate
        // placeholder an ingest has to tolerate silently.
        let key = key_with(|k| k.push_date("0500-01-01"));
        assert!(!key.is_valid);
        assert!(key.invalid_reason.is_none());
    }

    // ==================== Unsigned longs ====================

    // The bytes and routing hash (as an `i64`) of each value's key.
    const ULONG_KEYS: &[(&str, &str, i64)] = &[
        ("0", "0000000000000000", 3478107235931676136),
        ("1", "0100000000000000", 1967286128051477038),
        ("256", "0001000000000000", 8985795910368437836),
        ("9223372036854775807", "ffffffffffffff7f", -336274049380840307),
        ("9223372036854775808", "0000000000000080", 1756303889014446759),
        ("18446744073709551615", "ffffffffffffffff", 7895661345696739197),
        // Twenty digits above u64::MAX keep their low 64 bits.
        ("18446744073709551616", "0000000000000000", 3478107235931676136),
        ("99999999999999999999", "ffff0f632d5ec76b", -874268977331904208),
        ("00000000000000000001", "0100000000000000", 1967286128051477038),
    ];

    #[test]
    fn a_ulong_is_keyed_by_its_value() {
        for &(value, bytes, hash) in ULONG_KEYS {
            let mut key = RecordKey::new(8);
            key.push_ulong(value).unwrap();
            key.compute_hashes();
            let hex: String = key.bytes().iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(hex, bytes, "bytes of {value}");
            assert_eq!(key.routing_hash() as i64, hash, "hash of {value}");
            assert!(key.is_valid());
        }
    }

    #[test]
    fn a_ulong_that_is_not_digits_is_refused() {
        // The last is two Arabic-Indic digits, which the server refuses.
        for value in ["", "abc", "+5", "-1", " 1", "123456789012345678901", "\u{661}\u{662}"] {
            let mut key = RecordKey::new(8);
            let err = key.push_ulong(value).unwrap_err();
            assert_eq!(
                err.message(),
                format!(
                    "Value must be a positive integer less than or equal to the maximum unsigned \
                     long value, got {value}"
                )
            );
            assert!(key.bytes().is_empty(), "nothing is written for {value:?}");
        }
    }

    // ==================== Dates, times and addresses ====================

    // The bytes and validity of each value's key.
    const TEXT_KEYS: &[(&str, &str, &str, bool)] = &[
        ("time", "1:02:03", "00c02004", true),
        ("time", "01:02:03", "00c02004", true),
        ("time", "23:59:59.999", "70febe5f", true),
        ("time", "24:00:00", "00000000", false),
        ("time", "12:60:00", "00000000", false),
        ("time", "12:00:60", "00000000", false),
        ("time", "12:00:00.5", "00000000", false),
        ("time", "12:00:00.12", "00000000", false),
        ("time", "12:00:00.1234", "00000000", false),
        ("time", "\u{661}\u{662}:00:00", "00000000", false),
        ("time", "12:00:00\n", "00000000", false),
        ("datetime", "2020-06-15 12:30:45.123", "40a7f6683df60c0f", true),
        ("datetime", "2020-06-15 1:30:45", "40a70068bdf00c0f", true),
        ("datetime", "2020-06-15  12:30:45", "0000000000000000", false),
        ("datetime", "2020-06-15\t12:30:45", "0000000000000000", false),
        ("datetime", "2020-12-31 24:00:00", "c001000000fc190f", true),
        ("datetime", "2020-06-15 23:99:99", "60a80018c7fb0c0f", true),
        ("datetime", "2020-02-30 12:00:00", "203d000000e6050f", true),
        ("datetime", "2020-06-15 12:30:45.5", "0000000000000000", false),
        ("datetime", "2020-06-15 99:00:00", "c0ab000080f10c0f", true),
        ("datetime", "\u{662}020-06-15 12:00:00", "0000000000000000", false),
        ("date", "2020-06-15", "3af50c0f", true),
        ("date", "2020-13-40", "43811a0f", true),
        ("date", "\u{662}\u{660}\u{662}\u{660}-06-15", "00000000", false),
        ("ipv4", "10.0.0.1", "0100000a", true),
        ("ipv4", "255.255.255.255", "ffffffff", true),
        ("ipv4", "999.1.1.1", "00000000", false),
        ("ipv4", "1.2.3.256", "00000000", false),
        ("ipv4", "\u{661}.2.3.4", "00000000", false),
        ("ipv4", "01.002.3.4", "04030201", true),
    ];

    #[test]
    fn dates_times_and_addresses_are_keyed_by_their_fields() {
        for &(kind, value, bytes, valid) in TEXT_KEYS {
            let key = key_with(|k| match kind {
                "date" => k.push_date(value),
                "datetime" => k.push_datetime(value),
                "time" => k.push_time(value),
                "ipv4" => k.push_ipv4(value),
                _ => unreachable!(),
            });
            let hex: String = key.bytes().iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!((hex.as_str(), key.is_valid), (bytes, valid), "{kind} {value:?}");
        }
    }
}
