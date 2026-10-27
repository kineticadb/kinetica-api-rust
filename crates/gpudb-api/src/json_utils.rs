//! Whether a text is a JSON object or array, and how a list of JSON records is
//! sent as one array.
//!
//! A text is read leniently: unquoted and single-quoted names and values, `;`
//! between members, and trailing, empty and leading commas are taken, and
//! whatever follows the outermost object or array is ignored. A name twice in
//! one object is not taken. Only whether the text parses is answered.
//!
//! The parse keeps its own stack, so it has no limit on nesting depth.

use std::collections::HashSet;

use crate::text_format::{double_to_string, equals_ignore_case};

/// Whether `text` parses as a JSON object, read leniently (see the
/// [module docs](crate::json_utils)).
pub fn is_valid_json(text: &str) -> bool {
    parse(text, Frame::Object(HashSet::new())).is_ok()
}

/// Whether every text parses as a JSON object, as [`is_valid_json`] answers.
pub fn is_valid_json_list<S: AsRef<str>>(texts: impl IntoIterator<Item = S>) -> bool {
    texts.into_iter().all(|text| is_valid_json(text.as_ref()))
}

/// Whether `text` parses as a JSON array, read leniently (see the
/// [module docs](crate::json_utils)).
pub fn is_valid_json_array(text: &str) -> bool {
    parse(text, Frame::Array).is_ok()
}

/// The records, each already JSON, joined with commas inside brackets, as
/// they are without checking them.
pub fn to_json_array<S: AsRef<str>>(records: impl IntoIterator<Item = S>) -> String {
    let mut array = String::from("[");
    for (i, record) in records.into_iter().enumerate() {
        if i > 0 {
            array.push(',');
        }
        array.push_str(record.as_ref());
    }
    array.push(']');
    array
}

/// What the parse expects next.
enum Step {
    /// A member's name, or the end of the object.
    Name,
    /// A value.
    Value,
    /// The first element of an array, or its end.
    FirstElement,
    /// An element of an array, which may be left out.
    Element,
    /// What follows a member or an element.
    AfterValue,
    /// The end of the innermost object or array.
    Close,
}

/// An object or array being parsed; an object with the names it has, as
/// UTF-16 strings.
enum Frame {
    Object(HashSet<Vec<u16>>),
    Array,
}

/// Parses `text` as an object or an array, as `root` is one, failing where
/// it does not parse.
fn parse(text: &str, root: Frame) -> Result<(), ()> {
    let (open, mut step) = match root {
        Frame::Object(_) => ('{', Step::Name),
        Frame::Array => ('[', Step::FirstElement),
    };
    let mut x = Tokener::new(text);
    if x.next_clean() != open {
        return Err(());
    }
    let mut stack = vec![root];
    loop {
        step = match step {
            Step::Name => match x.next_clean() {
                '\0' => return Err(()),
                '}' => Step::Close,
                c => {
                    let name = x.next_simple_value(c)?;
                    if x.next_clean() != ':' {
                        return Err(());
                    }
                    let Some(Frame::Object(names)) = stack.last_mut() else { return Err(()) };
                    // A name given twice is an error
                    if !names.insert(name) {
                        return Err(());
                    }
                    Step::Value
                }
            },
            Step::Value => match x.next_clean() {
                '[' => {
                    x.back()?;
                    x.next_clean();
                    stack.push(Frame::Array);
                    Step::FirstElement
                }
                '{' => {
                    x.back()?;
                    x.next_clean();
                    stack.push(Frame::Object(HashSet::new()));
                    Step::Name
                }
                c => {
                    x.next_simple_value(c)?;
                    Step::AfterValue
                }
            },
            Step::FirstElement => match x.next_clean() {
                '\0' => return Err(()),
                ']' => Step::Close,
                _ => {
                    x.back()?;
                    Step::Element
                }
            },
            // A comma where an element would be leaves it out
            Step::Element => {
                let comma = x.next_clean() == ',';
                x.back()?;
                if comma {
                    Step::AfterValue
                } else {
                    Step::Value
                }
            }
            Step::AfterValue => match stack.last() {
                Some(Frame::Object(_)) => match x.next_clean() {
                    ',' => {
                        if x.next_clean() == '}' {
                            Step::Close
                        } else if x.end() {
                            return Err(());
                        } else {
                            x.back()?;
                            Step::Name
                        }
                    }
                    ';' => Step::Name,
                    '}' => Step::Close,
                    _ => return Err(()),
                },
                Some(Frame::Array) => match x.next_clean() {
                    ',' => match x.next_clean() {
                        '\0' => return Err(()),
                        ']' => Step::Close,
                        _ => {
                            x.back()?;
                            Step::Element
                        }
                    },
                    ']' => Step::Close,
                    _ => return Err(()),
                },
                None => return Err(()),
            },
            // Whatever follows the outermost object or array is not read
            Step::Close => {
                stack.pop();
                if stack.is_empty() {
                    return Ok(());
                }
                Step::AfterValue
            }
        }
    }
}

/// Reads characters one at a time with one step back. A NUL reads as the end
/// of the text, but what follows it can still be read.
struct Tokener {
    chars: Vec<char>,
    position: usize,
    eof: bool,
    index: u64,
    previous: char,
    use_previous: bool,
}

impl Tokener {
    fn new(text: &str) -> Self {
        Tokener { chars: text.chars().collect(), position: 0, eof: false, index: 0, previous: '\0', use_previous: false }
    }

    /// The next character, or NUL at the end.
    fn next(&mut self) -> char {
        let c = if self.use_previous {
            self.use_previous = false;
            self.previous
        } else {
            match self.chars.get(self.position) {
                Some(&c) => {
                    self.position += 1;
                    c
                }
                None => '\0',
            }
        };
        if c == '\0' {
            self.eof = true;
            return '\0';
        }
        self.index += 1;
        self.previous = c;
        c
    }

    /// Steps back one character, but not two.
    fn back(&mut self) -> Result<(), ()> {
        if self.use_previous || self.index == 0 {
            return Err(());
        }
        self.index -= 1;
        self.use_previous = true;
        self.eof = false;
        Ok(())
    }

    /// Whether the end has been read, with no step back pending.
    fn end(&self) -> bool {
        self.eof && !self.use_previous
    }

    /// The next character that is not white space or a control character.
    fn next_clean(&mut self) -> char {
        loop {
            let c = self.next();
            if c == '\0' || c > ' ' {
                return c;
            }
        }
    }

    /// The rest of a string quoted by `quote`, unescaped, as UTF-16 units.
    fn next_string(&mut self, quote: char) -> Result<Vec<u16>, ()> {
        let mut text = Vec::new();
        let push = |text: &mut Vec<u16>, c: char| text.extend(c.encode_utf16(&mut [0; 2]).iter());
        loop {
            match self.next() {
                '\0' | '\n' | '\r' => return Err(()),
                '\\' => match self.next() {
                    c @ ('"' | '\'' | '/' | '\\') => push(&mut text, c),
                    'b' => push(&mut text, '\u{8}'),
                    'f' => push(&mut text, '\u{c}'),
                    'n' => push(&mut text, '\n'),
                    'r' => push(&mut text, '\r'),
                    't' => push(&mut text, '\t'),
                    // The code is appended as one UTF-16 unit
                    'u' => text.push(parse_hex_int(&self.next_chars(4)?).ok_or(())? as u16),
                    _ => return Err(()),
                },
                c if c == quote => return Ok(text),
                c => push(&mut text, c),
            }
        }
    }

    /// The next `n` characters, failing at the end.
    fn next_chars(&mut self, n: usize) -> Result<String, ()> {
        let mut text = String::new();
        for _ in 0..n {
            text.push(self.next());
            if self.end() {
                return Err(());
            }
        }
        Ok(text)
    }

    /// A quoted string, or an unquoted run up to a delimiter, trimmed. Gives
    /// the value's text as it is written back (see [`string_to_value`]),
    /// which is what an object's names are compared by.
    fn next_simple_value(&mut self, first: char) -> Result<Vec<u16>, ()> {
        if first == '"' || first == '\'' {
            return self.next_string(first);
        }
        let mut run = String::new();
        let mut c = first;
        while c >= ' ' && !",:]}/\\\"[{;=#".contains(c) {
            run.push(c);
            c = self.next();
        }
        if !self.eof {
            self.back()?;
        }
        let run = run.trim_matches(|c: char| c <= ' ');
        if run.is_empty() {
            return Err(());
        }
        Ok(string_to_value(run).encode_utf16().collect())
    }
}

/// A hexadecimal integer: a sign, then hexadecimal digits.
fn parse_hex_int(text: &str) -> Option<u32> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let value = i64::from_str_radix(digits, 16).ok()?;
    Some((if negative { -value } else { value }) as u32)
}

/// The value an unquoted `text` stands for, as it is written back: `true`,
/// `false` and `null` in any case, a number as [`string_to_number`] writes
/// it, or else the text itself.
fn string_to_value(text: &str) -> String {
    for word in ["true", "false", "null"] {
        if equals_ignore_case(text, word) {
            return word.to_string();
        }
    }
    let starts_number = text.starts_with(|c: char| c.is_ascii_digit() || c == '-');
    // Longer than 1,000 characters, it is left as text
    if starts_number && text.encode_utf16().count() <= 1000 {
        if let Some(number) = string_to_number(text) {
            return number;
        }
    }
    text.to_string()
}

/// The number `text` stands for, as its text, or `None` where it is not
/// one.
fn string_to_number(text: &str) -> Option<String> {
    let decimal = text.contains(['.', 'e', 'E']) || text == "-0";
    if decimal {
        return match BigDecimal::parse(text) {
            // A negative zero is written as a double
            Some(value) if text.starts_with('-') && value.is_zero() => Some("-0.0".to_string()),
            Some(value) => Some(value.to_string()),
            None => parse_double(text).filter(|v| v.is_finite()).map(double_to_string),
        };
    }
    // A leading zero before another digit is not a number
    let bytes = text.as_bytes();
    let leading_zero = match bytes {
        [b'0', d, ..] => d.is_ascii_digit(),
        [b'-', b'0', d, ..] => d.is_ascii_digit(),
        _ => false,
    };
    if leading_zero {
        return None;
    }
    // An integer is written back as the very digits read
    let digits = text.strip_prefix('-').unwrap_or(text);
    (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then(|| text.to_string())
}

/// A decimal as its digits and scale.
struct BigDecimal {
    negative: bool,
    /// The unscaled value's digits, without leading zeros; `0` for zero.
    digits: String,
    scale: i32,
}

impl BigDecimal {
    /// The decimal `text` writes, or `None` where it is not one.
    fn parse(text: &str) -> Option<BigDecimal> {
        let (negative, rest) = match text.as_bytes().first()? {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let (significand, exponent) = match rest.find(['e', 'E']) {
            Some(at) => (&rest[..at], Some(&rest[at + 1..])),
            None => (rest, None),
        };
        let (whole, fraction) = significand.split_once('.').unwrap_or((significand, ""));
        if whole.is_empty() && fraction.is_empty() {
            return None;
        }
        if !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit()) {
            return None;
        }
        let exponent = match exponent {
            Some(exponent) => parse_exponent(exponent)?,
            None => 0,
        };
        let scale = i32::try_from(fraction.len() as i64 - exponent).ok()?;
        let digits = format!("{whole}{fraction}");
        let digits = digits.trim_start_matches('0');
        let digits = if digits.is_empty() { "0" } else { digits };
        Some(BigDecimal { negative, digits: digits.to_string(), scale })
    }

    fn is_zero(&self) -> bool {
        self.digits == "0"
    }
}

impl std::fmt::Display for BigDecimal {
    /// Plain, unless the scale is negative or the number is smaller than
    /// 10^-6, then in scientific notation.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.negative && !self.is_zero() {
            f.write_str("-")?;
        }
        let digits = self.digits.as_str();
        if self.scale == 0 {
            return f.write_str(digits);
        }
        let adjusted = -i64::from(self.scale) + (digits.len() as i64 - 1);
        if self.scale > 0 && adjusted >= -6 {
            let scale = self.scale as usize;
            return if digits.len() > scale {
                let (whole, fraction) = digits.split_at(digits.len() - scale);
                write!(f, "{whole}.{fraction}")
            } else {
                write!(f, "0.{}{digits}", "0".repeat(scale - digits.len()))
            };
        }
        f.write_str(&digits[..1])?;
        if digits.len() > 1 {
            write!(f, ".{}", &digits[1..])?;
        }
        if adjusted != 0 {
            write!(f, "E{}{adjusted}", if adjusted > 0 { "+" } else { "" })?;
        }
        Ok(())
    }
}

/// `BigDecimal`'s exponent: a sign and at most ten digits after leading
/// zeros.
fn parse_exponent(text: &str) -> Option<i64> {
    let (negative, digits) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let significant = if digits.len() > 10 { digits.trim_start_matches('0') } else { digits };
    if significant.len() > 10 {
        return None;
    }
    let value: i64 = if significant.is_empty() { 0 } else { significant.parse().ok()? };
    Some(if negative { -value } else { value })
}

/// The double `text` writes, or `None` where it is not one: a decimal or a
/// hexadecimal floating point literal, with an optional `f` or `d` suffix.
fn parse_double(text: &str) -> Option<f64> {
    let text = text.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(text);
    let (negative, rest) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let magnitude = match rest.strip_prefix("0x").or_else(|| rest.strip_prefix("0X")) {
        Some(hex) => hex_float(hex)?,
        None => decimal_float(rest)?,
    };
    Some(if negative { -magnitude } else { magnitude })
}

/// A decimal floating point literal's value: digits with at most one point,
/// at least one digit, and an optional exponent.
fn decimal_float(text: &str) -> Option<f64> {
    let (significand, exponent) = match text.find(['e', 'E']) {
        Some(at) => (&text[..at], Some(&text[at + 1..])),
        None => (text, None),
    };
    let (whole, fraction) = significand.split_once('.').unwrap_or((significand, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    if let Some(exponent) = exponent {
        let digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
    }
    let normalized = format!(
        "{}.{}e{}",
        if whole.is_empty() { "0" } else { whole },
        if fraction.is_empty() { "0" } else { fraction },
        exponent.unwrap_or("0")
    );
    normalized.parse().ok()
}

/// A hexadecimal floating point literal's value, after its `0x`: hex digits
/// with at most one point, at least one digit, and a binary exponent.
fn hex_float(text: &str) -> Option<f64> {
    let at = text.find(['p', 'P'])?;
    let (significand, exponent) = (&text[..at], &text[at + 1..]);
    let (whole, fraction) = significand.split_once('.').unwrap_or((significand, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.chars().chain(fraction.chars()).all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let exponent_digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
    if exponent_digits.is_empty() || !exponent_digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let exponent: i64 = exponent.parse().unwrap_or(if exponent.starts_with('-') { i64::MIN / 2 } else { i64::MAX / 2 });

    // The digits as one integer, scaled by a power of two
    let mut mantissa: u128 = 0;
    let mut shift: i64 = exponent;
    for (i, c) in whole.chars().chain(fraction.chars()).enumerate() {
        let in_fraction = i >= whole.len();
        if mantissa >> 120 == 0 {
            mantissa = mantissa * 16 + c.to_digit(16)? as u128;
            if in_fraction {
                shift -= 4;
            }
        } else if !in_fraction {
            shift += 4;
        }
    }
    Some(scale_by_power_of_two(mantissa as f64, shift))
}

/// `value` times two to the power `exponent`.
fn scale_by_power_of_two(mut value: f64, mut exponent: i64) -> f64 {
    while exponent > 0 && value.is_finite() && value != 0.0 {
        let step = exponent.min(1000);
        value *= 2f64.powi(step as i32);
        exponent -= step;
    }
    while exponent < 0 && value != 0.0 {
        let step = exponent.max(-1000);
        value *= 2f64.powi(step as i32);
        exponent -= step;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_are_sent_as_one_array() {
        assert_eq!(to_json_array(["{\"a\":1}", "{\"a\":2}"]), "[{\"a\":1},{\"a\":2}]");
        assert_eq!(to_json_array(Vec::<String>::new()), "[]");
        assert_eq!(to_json_array(vec![String::from("x"), String::from("{")]), "[x,{]");
    }

    // Each text, and whether it is taken for an object.
    #[test]
    fn records_are_valid_where_org_json_takes_them() {
        let cases: &[(&str, bool)] = &[
        ("{}", true),
        ("{\"a\":1}", true),
        (" \t\n{\"a\":1}", true),
        ("{\"a\":1} trailing", true),
        ("{\"a\":1}}", true),
        ("[1]", false),
        ("", false),
        ("{", false),
        ("{\"a\"}", false),
        ("{\"a\":}", false),
        ("{\"a\":1,}", true),
        ("{\"a\":1,,\"b\":2}", false),
        ("{\"a\":1;\"b\":2}", true),
        ("{\"a\":1;}", true),
        ("{a:1}", true),
        ("{'a':'b'}", true),
        ("{\"a\":hello world}", true),
        ("{\"a\":hello\tworld}", false),
        ("{\"a\":\"x\",\"a\":\"y\"}", false),
        ("{\"1\":1,1:2}", false),
        ("{\"1E+2\":1,1e2:2}", false),
        ("{\"-0.0\":1,-0:2}", false),
        ("{\"true\":1,TRUE:2}", false),
        ("{\"false\":1,fal\u{17f}e:2}", false),
        ("{\"null\":1,Null:2}", false),
        ("{\"0.0000010\":1,1.0E-6:2}", false),
        ("{\"1.5\":1,1.5f:2}", false),
        ("{\"3.0\":1,0x1.8p1:2}", false),
        ("{\"240.0\":1,0x1ep3:2}", false),
        ("{\"0.0\":1,1e-2147483648:2}", false),
        ("{\"-0.0\":1,-1e-2147483648:2}", false),
        ("{\"1e99999999999\":1,1e99999999999:2}", false),
        ("{\"01\":1,01:2}", false),
        ("{\"1\":1,01:2}", true),
        ("{\"12.50\":1,00012.50:2}", false),
        ("{\"5\":1,5.:2}", false),
        ("{\"1E+5\":1,1.e5:2}", false),
        ("{\"a\":[1,2,]}", true),
        ("{\"a\":[,1]}", true),
        ("{\"a\":[,]}", true),
        ("{\"a\":[]}", true),
        ("{\"a\":[1 2]}", true),
        ("{\"a\":[1,,2]}", true),
        ("{\"a\":[}", false),
        ("{\"a\":{\"b\":{\"c\":[{\"d\":null}]}}}", true),
        ("{\"a\":\"\\u00e9\"}", true),
        ("{\"a\":\"\\u-123\"}", true),
        ("{\"a\":\"\\u+12f\"}", true),
        ("{\"a\":\"\\u12g4\"}", false),
        ("{\"a\":\"\\u12\"}", false),
        ("{\"a\":\"\\x\"}", false),
        ("{\"a\":\"line\nbreak\"}", false),
        ("{\"a\":\"tab\there\"}", true),
        ("{\"a\":\"\\/\"}", true),
        ("{\"a\":'it\\'s'}", true),
        ("{\"a\":1/2}", false),
        ("{\"a\":1#}", false),
        ("{\"a\"=1}", false),
        ("{\"a\":1=}", false),
        ("{\"a\":1\0}", true),
        ("{\"a\":1,\0\"b\":2}", false),
        ("{\"x\":1\0;\"b\":2}", false),
        ("{\"a\":\"\\u0041\"\0}", false),
        ("\0{\"a\":1}", false),
        ("{\"a\":tru\0e}", false),
        ("{\"a\":\"x\0\"}", false),
        ("{\"a\":1}\0garbage", true),
        ("{\"a\" : 1 , \"b\" : [ 1 , { } ] }", true),
        ("{\"\":1}", true),
        ("{ :1}", false),
        ("{\"a\":-}", true),
        ("{\"a\":.5}", true),
        ("{\"a\":NaN}", true),
        ("{\"a\":1e5000}", true),
        ("{\"a\":{}}", true),
        ("{\"a\":{\"a\":1,\"a\":2}}", false),
        ("{\"a\":{\"b\":1},\"b\":{\"b\":2}}", true),
        ("{\"a\":[{\"x\":1,\"x\":1}]}", false),
        ("{\"a\":\"x\"}{\"b\":1}", true),
        ("{\"a\"\n:1}", true),
        ("{\"a\":\n1\n}", true),
        ("{\"a\":1\r}", true),
        ("{\"a\":\"\\b\\f\\n\\r\\t\\\"\\\\\\/\\'\"}", true),
        ("{\"a\":\"x\" \"b\":1}", false),
        ("{\"a\":1,\"b\"}", false),
        ("{\"a\":{\"b\":1}", false),
        ("{\"a\" :[1,2],}", true),
        ("{'a':1, a:2}", false),
        ("{\" a\":1, a :2}", true),
        ("{a b:1}", true),
        ("{\"a\":[[[[]]]]}", true),
        ("{\"k\":\"v\",}", true),
        ("{,}", false),
        ("{;}", false),
        ("{\"a\":1,;}", false),
        ("{\"a\":1;;}", false),
        ("{\"a\":\"x\"\0,\"b\":2}", false),
        ("\u{feff}{\"a\":1}", false),
        ("{\"a\":\"\\u0000\"}", true),
        ("{\"a\":1,\"b\":2,\"a\":3}", false),
        ("{-:1, \"-\":2}", false),
        ("{-0:1,\"-0\":2}", true),
        ("{0e5:1,\"0E+5\":2}", false),
        ("{1e2147483648:1,\"1E+2147483648\":2}", false),
        ("{1.5e-2147483647:1,\"0.0\":2}", false),
        ("{100e-2:1,\"1.00\":2}", false),
        ("{\"x\":[1,2]  ,  \"y\":3}", true),
        ("{\"a\":[1;2]}", false),
        ("{\"a\":[1,2]x}", false),
        ("{\"\\u0061\":1,\"a\":2}", false),
        ("{'a\\\"b':1}", true),
        ("{\"a\":\"\\u00\"}", false),
        ("{\"a\":\"\\uD83D\\uDE00\"}", true),
        ("{\"a\":\"\u{1f600}\"}", true),
        ("{\"\u{1f600}\":1,\u{1f600}:2}", false),
        ("{\"\\uD83D\\uDE00\":1,\"\u{1f600}\":2}", false),
        ("{\"\\uD800\":1,\"\\uD801\":2}", true),
        ("{\"a\":1}   ", true),
        ("  ", false),
        ("{\"a\":[1,\0]}", false),
        ("{\"a\":[\0]}", false),
        ("{\"1.5E10\":1,1.5e10f:2}", false),
        ("{\"1.0E-7\":1,1e-7d:2}", false),
        ("{\"a\":1,\n}", true),
        ("{\"a\":\"b\"\t,\t\"c\":\"d\"}", true),
        ("{\"a\":1,\"b\":}", false),
        ("{\"a\":x\0y}", false),
        ("{\"a\":\"s\",\0}", false),
        ("{\"a\0\":1}", false),
        ("{a\0:1}", false),
        ("{\"x\":1\0;\"a\":\"\\u0041\"}", false),
        ("{\"x\":1;\"a\":\"\\u0041\"}", true),
        ("{\"a\":1}\n", true),
        ("{\"a\":tRuE}", true),
        ("{\"a\":[{},[],\"\",0,-1.5e3,true,null]}", true),
        ("{\"a\":\"unterminated}", false),
        ("{'a':\"mixed'}", false),
        ("{\"a\":\"\\u+-12\"}", false),
        ("{\"a\":\"\\u-fff\"}", true),
        ("{\"o\u{212a}\":1,\"OK\":2}", true),
        ("{OK:1,o\u{212a}:2}", true),
        ("{\"a\":\"b\",\u{1}\"c\":\"d\"}", true),
        ("{\u{1}\"a\":1}", true),
        ("{\"a\":1\u{1}}", true),
        ("{\"a\":x\u{1}}", true),
        ("{\"a\":x\u{1}y}", false),
        ("{\"a\":1e-7d}", true),
        ("{\"a\":-0x1.8p1}", true),
        ("{\"a\" :\"v\" ; \"b\":\"w\" , }", true),
        ("{\"a\":\"b\"};", true),
        ];
        let wrong: Vec<_> = cases.iter().filter(|(text, valid)| is_valid_json(text) != *valid).collect();
        assert!(wrong.is_empty(), "misjudged: {wrong:#?}");
    }

    // Each text, and whether it is taken for an array.
    #[test]
    fn arrays_are_valid_where_org_json_takes_them() {
        let cases: &[(&str, bool)] = &[
        ("[]", true),
        ("[1,2]", true),
        (" \t\n[1]", true),
        ("[1] trailing", true),
        ("[1]]", true),
        ("{}", false),
        ("", false),
        ("  ", false),
        ("[", false),
        ("[,]", true),
        ("[,1]", true),
        ("[1,]", true),
        ("[1,,2]", true),
        ("[1 2]", true),
        ("[1;2]", false),
        ("[{\"a\":1,\"a\":2}]", false),
        ("[{\"a\":1},{\"a\":2}]", true),
        ("[\0]", false),
        ("[1,\0]", false),
        ("[1]\0x", true),
        ("\0[1]", false),
        ("[[[]]]", true),
        ("[\"a\" \"b\"]", false),
        ("['a']", true),
        ("[a b]", true),
        ("[1}", false),
        ("[{]", false),
        ("[1,2", false),
        ("[}]", false),
        ("[]x", true),
        ("\u{feff}[1]", false),
        ("[\"x\0\"]", false),
        ("[null]", true),
        ("[-]", true),
        ("[,,]", true),
        ("[ , ]", true),
        ("[1,]x", true),
        ("[1],[2]", true),
        ("[{}]", true),
        ("[{\"a\":[1,{\"b\":2}]}]", true),
        ("[\"\\u12g4\"]", false),
        ("[\"line\nbreak\"]", false),
        ("[\u{1} 1]", true),
        ("[1\u{1}]", true),
        ("[]\0", true),
        ("[,\0]", false),
        ("[1 , ]", true),
        ("[:]", false),
        ("[1:2]", false),
        ("[=]", false),
        ("[#]", false),
        ("[/]", false),
        ("[tRuE, NaN, 1e5000]", true),
        ("[\"a\",'b',c]", true),
        ("[\n]", true),
        ("[\r\n1\r\n]", true),
        ("[[]", false),
        ("[]]", true),
        ("[[],]", true),
        ("[[],,]", true),
        ("[{}{}]", false),
        ("[{},{}]", true),
        ];
        let wrong: Vec<_> = cases.iter().filter(|(text, valid)| is_valid_json_array(text) != *valid).collect();
        assert!(wrong.is_empty(), "misjudged: {wrong:#?}");
    }

    /// A list is valid when every record in it is, and an empty one is.
    #[test]
    fn lists_are_valid_when_every_record_is() {
        assert!(is_valid_json_list(["{\"a\":1}", "{b:2}"]));
        assert!(is_valid_json_list(vec![String::from("{}")]));
        assert!(!is_valid_json_list(["{\"a\":1}", "[1]"]));
        assert!(is_valid_json_list(Vec::<String>::new()));
    }

    /// Names are compared as they are written back.
    #[test]
    fn unquoted_values_are_read_as_org_json_reads_them() {
        for (text, value) in [
            ("TRUE", "true"),
            ("falſe", "false"),
            ("Null", "null"),
            ("12", "12"),
            ("012", "012"),
            ("-012", "-012"),
            ("-0", "-0.0"),
            ("1e2", "1E+2"),
            ("1.", "1"),
            ("00012.50", "12.50"),
            ("1.0E-6", "0.0000010"),
            ("123456789e-15", "1.23456789E-7"),
            ("0e5", "0E+5"),
            ("0.000e-10", "0E-13"),
            ("1e2147483648", "1E+2147483648"),
            ("1e-2147483648", "0.0"),
            ("-1e-2147483648", "-0.0"),
            ("1e99999999999", "1e99999999999"),
            ("1.5f", "1.5"),
            ("1e5d", "100000.0"),
            ("0x1.8p1", "3.0"),
            ("0x1ep3", "240.0"),
            ("0x10", "0x10"),
            ("1_0.0", "1_0.0"),
            ("-", "-"),
            (".5", ".5"),
            ("-.5", "-0.5"),
            ("hello world", "hello world"),
        ] {
            assert_eq!(string_to_value(text), value, "{text}");
        }
    }
}
