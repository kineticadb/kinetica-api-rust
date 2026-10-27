//! The text forms of floating point numbers that the client writes into
//! filter expressions and parses back as decimals.
//!
//! The text always has a fraction (`1.0`), and switches to `E` notation below
//! `10^-3` and from `10^7` on (`1.0E7`), where Rust's own formatting does
//! neither.
//!
//! The digits are the shortest that identify the number, which Rust's own
//! formatting already finds, with one rule added: where a single digit would
//! do, the text has the two-digit decimal closest to the number if that also
//! identifies it. That can only differ for a subnormal number, whose few bits
//! leave room for more than one such decimal (`4.9E-324`, where the shortest is
//! `5E-324`).
//!
//! Also case-insensitive equality, by which the client compares names and
//! answers.

use num_bigint::BigUint;

/// Whether `a` and `b` are equal ignoring case: the same length in UTF-16
/// units, and each pair of characters equal, or equal in upper case, or equal
/// in lower case after that. Each character maps to a single character (one
/// that would map to several stays as it is), so `ſ` matches `s` and the
/// Kelvin sign matches `k`.
pub(crate) fn equals_ignore_case(a: &str, b: &str) -> bool {
    fn single(mut mapped: impl Iterator<Item = char>, c: char) -> char {
        match (mapped.next(), mapped.next()) {
            (Some(m), None) => m,
            _ => c,
        }
    }
    let upper = |c: char| single(c.to_uppercase(), c);
    let lower = |c: char| single(c.to_lowercase(), c);
    a.encode_utf16().count() == b.encode_utf16().count()
        && a.chars().count() == b.chars().count()
        && a.chars().zip(b.chars()).all(|(x, y)| {
            x == y || upper(x) == upper(y) || lower(upper(x)) == lower(upper(y))
        })
}

/// The text of `value`, in the form the module describes.
pub(crate) fn double_to_string(value: f64) -> String {
    let magnitude = value.abs();
    let subnormal = (magnitude != 0.0 && magnitude < f64::MIN_POSITIVE)
        .then(|| Subnormal { significand: magnitude.to_bits(), unit_exponent: 1075 });
    number_text(
        value.is_nan(),
        value.is_infinite(),
        value.is_sign_negative(),
        magnitude == 0.0,
        || format!("{magnitude:e}"),
        (1e-3..1e7).contains(&magnitude),
        subnormal,
    )
}

/// The text of `value`, in the form the module describes.
pub(crate) fn float_to_string(value: f32) -> String {
    let magnitude = value.abs();
    let subnormal = (magnitude != 0.0 && magnitude < f32::MIN_POSITIVE)
        .then(|| Subnormal { significand: u64::from(magnitude.to_bits()), unit_exponent: 150 });
    number_text(
        value.is_nan(),
        value.is_infinite(),
        value.is_sign_negative(),
        magnitude == 0.0,
        || format!("{magnitude:e}"),
        (1e-3..1e7).contains(&magnitude),
        subnormal,
    )
}

/// A subnormal number: `significand` times 2 to the minus `unit_exponent`,
/// halved. So the number is `2 * significand` units, and the decimals that
/// round to it lie between `2 * significand - 1` and `2 * significand + 1`
/// units.
struct Subnormal {
    significand: u64,
    unit_exponent: u32,
}

fn number_text(
    nan: bool,
    infinite: bool,
    negative: bool,
    zero: bool,
    shortest: impl FnOnce() -> String,
    plain: bool,
    subnormal: Option<Subnormal>,
) -> String {
    if nan {
        return "NaN".to_string();
    }
    let sign = if negative { "-" } else { "" };
    if infinite {
        return format!("{sign}Infinity");
    }
    if zero {
        return format!("{sign}0.0");
    }

    // Rust's shortest form, as `d.ddde<exponent>`
    let shortest = shortest();
    let (mantissa, exponent) = shortest.split_once('e').expect("exponent form");
    let mut digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let mut exponent: i32 = exponent.parse().expect("exponent");

    if digits.len() == 1 {
        if let Some((two_digits, at)) = subnormal.and_then(|s| closest_two_digits(&s, exponent)) {
            digits = two_digits;
            exponent = at;
        }
    }

    let (first, rest) = digits.split_at(1);
    if !plain {
        let fraction = if rest.is_empty() { "0" } else { rest };
        return format!("{sign}{first}.{fraction}E{exponent}");
    }
    if exponent >= 0 {
        let whole_length = exponent as usize + 1;
        if digits.len() <= whole_length {
            format!("{sign}{digits}{}.0", "0".repeat(whole_length - digits.len()))
        } else {
            format!("{sign}{}.{}", &digits[..whole_length], &digits[whole_length..])
        }
    } else {
        format!("{sign}0.{}{digits}", "0".repeat((-exponent - 1) as usize))
    }
}

/// The two-digit decimal written for a subnormal number whose shortest
/// form has a single digit, at `exponent`: the two-digit decimal closest to
/// the number, if it still rounds to the number and is not that single digit
/// itself. Ties go to the even digit. Returns its digits and exponent.
fn closest_two_digits(number: &Subnormal, exponent: i32) -> Option<(String, i32)> {
    let ten = |power: i32| BigUint::from(10u32).pow(power.unsigned_abs());
    let unit = BigUint::from(1u32) << number.unit_exponent;
    let x = BigUint::from(2 * number.significand);

    // The decade of the number: 10^decade <= x < 10^(decade + 1). The single
    // digit may have rounded up into the next decade.
    let decade = if &x * ten(exponent) >= unit { exponent } else { exponent - 1 };

    // The two-digit decimals c * 10^(decade - 1) either side of the number,
    // compared with it as c * unit against units * 10^(1 - decade)
    let scale = ten(decade - 1);
    let below = &x * &scale / &unit;
    let above = &below + 1u32;
    let scaled = |units: &BigUint| units * &scale;
    let even = number.significand.is_multiple_of(2);
    let rounds_to_number = |c: &BigUint| {
        let d = c * &unit;
        let low = scaled(&(&x - 1u32));
        let high = scaled(&(&x + 1u32));
        if even { d >= low && d <= high } else { d > low && d < high }
    };

    let below_ok = rounds_to_number(&below);
    let above_ok = rounds_to_number(&above);
    let chosen = match (below_ok, above_ok) {
        (true, true) => {
            let from_below = scaled(&x) - &below * &unit;
            let to_above = &above * &unit - scaled(&x);
            match from_below.cmp(&to_above) {
                std::cmp::Ordering::Less => below,
                std::cmp::Ordering::Greater => above,
                std::cmp::Ordering::Equal => {
                    if (&below % 2u32) == BigUint::from(0u32) { below } else { above }
                }
            }
        }
        (true, false) => below,
        (false, true) => above,
        (false, false) => return None,
    };

    // A multiple of ten is a single digit, which the shortest form already is
    if (&chosen % 10u32) == BigUint::from(0u32) {
        return None;
    }
    Some((chosen.to_string(), decade))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_is_ignored_char_by_char() {
        assert!(equals_ignore_case("OK", "ok"));
        assert!(equals_ignore_case("falſe", "FALSE"));
        assert!(equals_ignore_case("o\u{212A}", "OK"));
        assert!(!equals_ignore_case("OK", "OKAY"));
        assert!(!equals_ignore_case("straße", "STRASSE"));
    }

    #[test]
    fn doubles_print_in_their_shortest_form() {
        for (value, expected) in [
            (1.0, "1.0"),
            (-1.0, "-1.0"),
            (0.1, "0.1"),
            (100.0, "100.0"),
            (1234567.0, "1234567.0"),
            (9999999.0, "9999999.0"),
            (1e7, "1.0E7"),
            (12345678.0, "1.2345678E7"),
            (0.001, "0.001"),
            (0.00123, "0.00123"),
            (1e-4, "1.0E-4"),
            (1.5e-10, "1.5E-10"),
            (2e23, "2.0E23"),
            (123.456, "123.456"),
            (f64::MAX, "1.7976931348623157E308"),
            (f64::MIN_POSITIVE, "2.2250738585072014E-308"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert_eq!(double_to_string(value), expected, "{value:e}");
        }
    }

    // The least positive double is written `4.9E-324`, where the shortest
    // decimal is `5E-324`: the closer two-digit decimal is taken.
    #[test]
    fn a_subnormal_double_takes_the_closest_two_digits() {
        assert_eq!(double_to_string(f64::from_bits(1)), "4.9E-324");
        assert_eq!(double_to_string(f64::from_bits(2)), "9.9E-324");
        assert_eq!(double_to_string(f64::from_bits(3)), "1.5E-323");
        assert_eq!(double_to_string(-f64::from_bits(1)), "-4.9E-324");
    }

    #[test]
    fn floats_print_in_their_shortest_form() {
        for (value, expected) in [
            (1.0f32, "1.0"),
            (0.1, "0.1"),
            (1e10, "1.0E10"),
            (3.4028235e38, "3.4028235E38"),
            (f32::MIN_POSITIVE, "1.1754944E-38"),
            (f32::from_bits(1), "1.4E-45"),
            (-2.5, "-2.5"),
            (f32::NAN, "NaN"),
        ] {
            assert_eq!(float_to_string(value), expected, "{value:e}");
        }
    }
}
