//! Exact decimal arithmetic for shard key values.
//!
//! A decimal key value is encoded from its text parsed exactly, rounded half
//! up, so every client puts a record on the same rank. Parsing through `f64`
//! instead loses exactness: `1.005` at scale 2 is 101 parsed exactly (HALF_UP)
//! but 100 through a double, whose nearest value is just below 1.005.
//!
//! Only ASCII digits are accepted.

use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};

/// The precision and scale of a `decimal` column whose property gives none,
/// the only column type the "legacy" decimal range applies to.
const LEGACY_PRECISION: usize = 18;
const LEGACY_SCALE: usize = 4;

/// A decimal held exactly: `unscaled × 10^-scale`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Decimal {
    unscaled: BigInt,
    scale: i64,
}

fn pow10(exponent: u64) -> BigInt {
    num_traits::pow(BigInt::from(10), exponent as usize)
}

/// The number of decimal digits in `value`'s magnitude (so 0 has one).
fn digit_count(value: &BigInt) -> usize {
    value.magnitude().to_string().len()
}

/// Parses the exponent of a decimal's text: an optional sign, then at least
/// one digit, fitting in an `i32`.
fn parse_exponent(text: &str) -> Option<i64> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let value: i64 = text.parse().ok()?;
    i32::try_from(value).ok().map(i64::from)
}

impl Decimal {
    /// Parses `text` as a decimal, or `None` where it is not one: an optional
    /// sign, digits with at most one decimal point (at least one digit in
    /// all), and an optional exponent.
    fn parse(text: &str) -> Option<Self> {
        let (negative, rest) = match text.as_bytes().first() {
            Some(b'-') => (true, &text[1..]),
            Some(b'+') => (false, &text[1..]),
            _ => (false, text),
        };
        let (significand, exponent) = match rest.find(['e', 'E']) {
            Some(at) => (&rest[..at], parse_exponent(&rest[at + 1..])?),
            None => (rest, 0),
        };

        let mut digits = String::with_capacity(significand.len());
        let mut fraction_digits: i64 = 0;
        let mut seen_point = false;
        for b in significand.bytes() {
            match b {
                b'0'..=b'9' => {
                    digits.push(char::from(b));
                    if seen_point {
                        fraction_digits += 1;
                    }
                }
                b'.' if !seen_point => seen_point = true,
                _ => return None,
            }
        }
        if digits.is_empty() {
            return None;
        }

        let scale = fraction_digits - exponent;
        i32::try_from(scale).ok()?;

        let magnitude: BigInt = digits.parse().ok()?;
        let unscaled = if negative { -magnitude } else { magnitude };
        Some(Self { unscaled, scale })
    }

    /// The value at `scale`, rounded half up, as an unscaled integer, or
    /// `None` when the result would have more than `max_digits` digits.
    ///
    /// The digit cap only spares building a number that is about to be
    /// rejected for its length, which an exponent such as `1e999999999` would
    /// make enormous; it does not change which values are accepted.
    fn rescaled_half_up(&self, scale: i64, max_digits: usize) -> Option<BigInt> {
        if self.unscaled.is_zero() {
            return Some(BigInt::zero());
        }
        let digits = digit_count(&self.unscaled) as i64;
        let shift = scale - self.scale;
        if shift >= 0 {
            if digits + shift > max_digits as i64 {
                return None;
            }
            return Some(&self.unscaled * pow10(shift as u64));
        }

        let drop = (-shift) as u64;
        if drop > digits as u64 {
            // Less than a tenth in magnitude, so HALF_UP gives zero.
            return Some(BigInt::zero());
        }
        let divisor = pow10(drop);
        let magnitude = self.unscaled.abs();
        let mut quotient = &magnitude / &divisor;
        let remainder = &magnitude % &divisor;
        if remainder * 2 >= divisor {
            quotient += 1;
        }
        Some(if self.unscaled.is_negative() { -quotient } else { quotient })
    }

    /// The value at `scale`, or `None` where digits other than zero would be
    /// dropped.
    fn rescaled_exactly(&self, scale: i64) -> Option<Self> {
        let shift = scale - self.scale;
        let unscaled = if shift >= 0 {
            &self.unscaled * pow10(shift as u64)
        } else {
            let divisor = pow10((-shift) as u64);
            if !(&self.unscaled % &divisor).is_zero() {
                return None;
            }
            &self.unscaled / divisor
        };
        Some(Self { unscaled, scale })
    }

    /// The value's text without an exponent, for a non-negative scale.
    fn to_plain_string(&self) -> String {
        let digits = self.unscaled.magnitude().to_string();
        let sign = if self.unscaled.is_negative() { "-" } else { "" };
        let scale = self.scale.max(0) as usize;
        if scale == 0 {
            return format!("{sign}{digits}");
        }
        let padded = format!("{digits:0>width$}", width = scale + 1);
        let (whole, fraction) = padded.split_at(padded.len() - scale);
        format!("{sign}{whole}.{fraction}")
    }

    /// Whether this is a `decimal(18,4)` value that still fits the 8 bytes
    /// such a column used before 12-byte decimals, though it has 19 digits.
    fn is_supported_legacy_value(&self, precision: usize, scale: usize) -> bool {
        if precision != LEGACY_PRECISION || scale != LEGACY_SCALE {
            return false;
        }
        // value ∈ [i64::MIN, i64::MAX] × 10^-4, compared exactly:
        // value × 10^4 against the i64 range, scaled to a common exponent.
        let shift = LEGACY_SCALE as i64 - self.scale;
        let (value, min, max) = if shift >= 0 {
            (&self.unscaled * pow10(shift as u64), BigInt::from(i64::MIN), BigInt::from(i64::MAX))
        } else {
            let factor = pow10((-shift) as u64);
            (self.unscaled.clone(), BigInt::from(i64::MIN) * &factor, BigInt::from(i64::MAX) * factor)
        };
        value >= min && value <= max
    }
}

/// `text` scaled to `scale` with HALF_UP rounding, as an unscaled integer, or
/// `None` when the text is not a number or has more than `precision` digits
/// at that scale (unless it is a legacy `decimal(18,4)` value).
pub(crate) fn parse_decimal_value(text: &str, precision: usize, scale: usize) -> Option<BigInt> {
    let decimal = Decimal::parse(text)?;
    // A legacy value can have one digit more than its precision.
    let unscaled = decimal.rescaled_half_up(scale as i64, precision + 1)?;
    if digit_count(&unscaled) > precision && !decimal.is_supported_legacy_value(precision, scale) {
        return None;
    }
    Some(unscaled)
}

/// The text of `number` (itself the text of an integer or a finite double)
/// at `scale`, or `None` where the number has digits beyond the scale.
///
/// Trailing zeros after a decimal point are stripped, leaving the point
/// ("120.00" is "120."). Only zeros after a decimal point are stripped, so a
/// whole number at scale 0 keeps its value.
pub(crate) fn number_text_at_scale(number: &str, scale: usize) -> Option<String> {
    let plain = Decimal::parse(number)?.rescaled_exactly(scale as i64)?.to_plain_string();
    Some(if plain.contains('.') { plain.trim_end_matches('0').to_string() } else { plain })
}

/// `value` reduced to its low `bits` bits, two's complement.
pub(crate) fn low_bits(value: &BigInt, bits: u32) -> (u64, u32) {
    let modulus = num_traits::pow(BigInt::from(2), bits as usize);
    let mut wrapped = value % &modulus;
    if wrapped.is_negative() {
        wrapped += &modulus;
    }
    let two_64 = num_traits::pow(BigInt::from(2), 64);
    let low = (&wrapped % &two_64).to_u64().unwrap_or(0);
    let high = (&wrapped / two_64).to_u32().unwrap_or(0);
    (low, high)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(text: &str) -> Option<(i64, i64)> {
        Decimal::parse(text).map(|d| (d.unscaled.to_i64().unwrap(), d.scale))
    }

    #[test]
    fn text_is_parsed_as_big_decimal_parses_it() {
        assert_eq!(parsed("123.45"), Some((12345, 2)));
        assert_eq!(parsed("-0.5"), Some((-5, 1)));
        assert_eq!(parsed("+7"), Some((7, 0)));
        assert_eq!(parsed("1."), Some((1, 0)));
        assert_eq!(parsed(".5"), Some((5, 1)));
        assert_eq!(parsed("1E+3"), Some((1, -3)));
        assert_eq!(parsed("1.5e-2"), Some((15, 3)));
        assert_eq!(parsed("007"), Some((7, 0)));
        for bad in ["", "+", "-", ".", "1e", "e5", "1.2.3", " 1", "1 ", "abc", "1e+", "NaN", "1_000"] {
            assert_eq!(parsed(bad), None, "{bad:?}");
        }
        // The exponent has to fit in an int.
        assert_eq!(parsed("1e2147483648"), None);
    }

    #[test]
    fn values_round_half_up_away_from_zero() {
        let at = |text: &str, scale| parse_decimal_value(text, 18, scale).map(|v| v.to_i64().unwrap());
        // The case a double gets wrong: 1.005 is just below 1.005 in binary.
        assert_eq!(at("1.005", 2), Some(101));
        assert_eq!(at("-1.005", 2), Some(-101));
        assert_eq!(at("1.004", 2), Some(100));
        assert_eq!(at("2.5", 0), Some(3));
        assert_eq!(at("-2.5", 0), Some(-3));
        assert_eq!(at("1e2", 2), Some(10000));
        assert_eq!(at("0.0000001", 4), Some(0));
        assert_eq!(at("1e-999999999", 4), Some(0));
    }

    #[test]
    fn values_wider_than_the_precision_are_refused() {
        assert_eq!(parse_decimal_value("12345.678", 5, 2), None);
        assert_eq!(parse_decimal_value("123.456", 5, 2).map(|v| v.to_i64().unwrap()), Some(12346));
        assert_eq!(parse_decimal_value("1e999999999", 30, 4), None);
        assert_eq!(parse_decimal_value("abc", 18, 4), None);
    }

    #[test]
    fn decimal_18_4_keeps_its_legacy_19_digit_range() {
        let max = "922337203685477.5807";
        let min = "-922337203685477.5808";
        assert_eq!(parse_decimal_value(max, 18, 4).map(|v| v.to_i64().unwrap()), Some(i64::MAX));
        assert_eq!(parse_decimal_value(min, 18, 4).map(|v| v.to_i64().unwrap()), Some(i64::MIN));
        assert_eq!(parse_decimal_value("922337203685477.5808", 18, 4), None);
        // Only decimal(18,4) has the legacy range.
        assert_eq!(parse_decimal_value("100000000000000000.0", 18, 1), None);
    }

    #[test]
    fn numbers_take_their_text_at_the_scale() {
        assert_eq!(number_text_at_scale("1.5", 4).as_deref(), Some("1.5"));
        assert_eq!(number_text_at_scale("120", 2).as_deref(), Some("120."));
        assert_eq!(number_text_at_scale("-0.005", 3).as_deref(), Some("-0.005"));
        // A whole number at scale 0 keeps its value.
        assert_eq!(number_text_at_scale("100", 0).as_deref(), Some("100"));
        assert_eq!(number_text_at_scale("0", 0).as_deref(), Some("0"));
        assert_eq!(number_text_at_scale("-120", 0).as_deref(), Some("-120"));
        assert_eq!(number_text_at_scale("1.5E2", 0).as_deref(), Some("150"));
        // Dropping a digit other than zero gives no text.
        assert_eq!(number_text_at_scale("1.234", 2), None);
        assert_eq!(number_text_at_scale("1.230", 2).as_deref(), Some("1.23"));
    }

    #[test]
    fn low_bits_wrap_as_twos_complement() {
        assert_eq!(low_bits(&BigInt::from(-1), 64), (u64::MAX, 0));
        assert_eq!(low_bits(&BigInt::from(-1), 96), (u64::MAX, u32::MAX));
        assert_eq!(low_bits(&BigInt::from(-10000), 96), ((-10000i64) as u64, u32::MAX));
        let big = BigInt::from(1u128 << 70) + 5;
        assert_eq!(low_bits(&big, 96), (5, 1 << 6));
    }
}
