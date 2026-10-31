//! InfluxDB Line Protocol parsing.
//!
//! The grammar this accepts is the one Telegraf and the InfluxDB 1.x clients
//! emit:
//!
//! ```text
//! measurement[,tag=value...] field=value[,...] [timestamp]
//! ```
//!
//! Everything is parsed by hand rather than with a parser-combinator crate so
//! the dependency set stays small; line protocol is a small enough grammar
//! that a combinator buys nothing here, and the project already prefers few
//! dependencies for cross-compilation.

use std::fmt;

/// A value from a field set, keeping the type the writer stated.
///
/// Line protocol distinguishes `1` (float) from `1i` (integer) from `"1"`
/// (string). Collapsing those into one numeric type would silently rewrite the
/// caller's data, so the distinction is preserved until it is mapped onto a
/// concrete [`tmdb_timestore::encoding::DataType`].
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Float(f64),
    Int(i64),
    UInt(u64),
    Bool(bool),
    Str(String),
}

/// One parsed line.
#[derive(Debug, Clone, PartialEq)]
pub struct LpPoint {
    pub measurement: String,
    pub tags: Vec<(String, String)>,
    pub fields: Vec<(String, FieldValue)>,
    /// `None` when the line carried no timestamp, meaning "server assigns one".
    pub timestamp: Option<i64>,
}

/// Timestamp precision, mirroring the `precision` query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precision {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl Precision {
    /// Scale a timestamp given in this precision into a table's own unit.
    ///
    /// Returns `None` on overflow rather than wrapping, because a wrapped
    /// timestamp would land the row in the wrong time range and look like real
    /// data.
    pub fn to_ts_unit(self, ts: i64, unit: tmdb_timestore::TsUnit) -> Option<i64> {
        use tmdb_timestore::TsUnit;
        match (self, unit) {
            (Precision::Seconds, TsUnit::Seconds) => Some(ts),
            (Precision::Seconds, TsUnit::Millis) => ts.checked_mul(1_000),
            (Precision::Seconds, TsUnit::Micros) => ts.checked_mul(1_000_000),
            (Precision::Seconds, TsUnit::Nanos) => ts.checked_mul(1_000_000_000),
            (Precision::Milliseconds, TsUnit::Seconds) => Some(ts / 1_000),
            (Precision::Milliseconds, TsUnit::Millis) => Some(ts),
            (Precision::Milliseconds, TsUnit::Micros) => ts.checked_mul(1_000),
            (Precision::Milliseconds, TsUnit::Nanos) => ts.checked_mul(1_000_000),
            (Precision::Microseconds, TsUnit::Seconds) => Some(ts / 1_000_000),
            (Precision::Microseconds, TsUnit::Millis) => Some(ts / 1_000),
            (Precision::Microseconds, TsUnit::Micros) => Some(ts),
            (Precision::Microseconds, TsUnit::Nanos) => ts.checked_mul(1_000),
            (Precision::Nanoseconds, TsUnit::Seconds) => Some(ts / 1_000_000_000),
            (Precision::Nanoseconds, TsUnit::Millis) => Some(ts / 1_000_000),
            (Precision::Nanoseconds, TsUnit::Micros) => Some(ts / 1_000),
            (Precision::Nanoseconds, TsUnit::Nanos) => Some(ts),
        }
    }
}

/// Why a line could not be parsed.
#[derive(Debug, Clone, PartialEq)]
pub enum LpError {
    /// The line was empty or held only a comment.
    Empty,
    /// No measurement name before the first space.
    MissingMeasurement,
    /// No field set between the tag set and the timestamp.
    MissingFields,
    /// A tag or field was written without `=`.
    MissingSeparator { part: &'static str },
    /// A field key was empty.
    EmptyKey { part: &'static str },
    /// A tag key or value was empty.
    EmptyTagPart,
    /// The field value was not a number, boolean, or quoted string.
    BadFieldValue { key: String, value: String },
    /// A quoted string field was never closed.
    UnterminatedString,
    /// The trailing timestamp was not an integer.
    BadTimestamp { got: String },
    /// The line ended with a dangling escape or stray separator.
    TrailingSeparator { part: &'static str },
}

impl fmt::Display for LpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LpError::Empty => write!(f, "empty line"),
            LpError::MissingMeasurement => write!(f, "no measurement name"),
            LpError::MissingFields => write!(f, "no field set"),
            LpError::MissingSeparator { part } => {
                write!(f, "{} entry is missing '='", part)
            }
            LpError::EmptyKey { part } => write!(f, "{} entry has an empty key", part),
            LpError::EmptyTagPart => write!(f, "tag key or value is empty"),
            LpError::BadFieldValue { key, value } => {
                write!(f, "field {:?} has unparseable value {:?}", key, value)
            }
            LpError::UnterminatedString => write!(f, "quoted string field is not closed"),
            LpError::BadTimestamp { got } => write!(f, "timestamp {:?} is not an integer", got),
            LpError::TrailingSeparator { part } => {
                write!(f, "{} ends with a stray separator", part)
            }
        }
    }
}

impl std::error::Error for LpError {}

/// Parse one line of line protocol.
pub fn parse_line(line: &str) -> Result<LpPoint, LpError> {
    let trimmed = line.trim_matches(|c: char| c == '\r' || c == '\n');
    let trimmed = trimmed.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Err(LpError::Empty);
    }

    // Split into (measurement+tags, fields, timestamp?) on unescaped spaces.
    // Spaces inside a quoted field value are data, not separators, so this has
    // to track quoting rather than call `split_whitespace`.
    let mut sections: Vec<&str> = Vec::with_capacity(3);
    let bytes = trimmed.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    while i < bytes.len() {
        let c = bytes[i];
        if escaped {
            escaped = false;
            i += 1;
            continue;
        }
        match c {
            b'\\' => escaped = true,
            b'"' => in_string = !in_string,
            b' ' if !in_string => {
                sections.push(&trimmed[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    if in_string {
        return Err(LpError::UnterminatedString);
    }
    sections.push(&trimmed[start..]);

    if sections.is_empty() || sections[0].is_empty() {
        return Err(LpError::MissingMeasurement);
    }
    if sections.len() < 2 {
        return Err(LpError::MissingFields);
    }

    // Section 1: measurement plus tag set.
    let mut head = split_unescaped(sections[0], b',');
    let measurement_raw = head.remove(0);
    if measurement_raw.is_empty() {
        return Err(LpError::MissingMeasurement);
    }
    let measurement = unescape(measurement_raw);

    let mut tags: Vec<(String, String)> = Vec::with_capacity(head.len());
    for pair in head {
        if pair.is_empty() {
            return Err(LpError::TrailingSeparator { part: "tag set" });
        }
        let (k, v) = split_once_unescaped(pair, b'=')
            .ok_or(LpError::MissingSeparator { part: "tag" })?;
        if k.is_empty() || v.is_empty() {
            return Err(LpError::EmptyTagPart);
        }
        tags.push((unescape(k), unescape(v)));
    }

    // Section 2: field set.
    let mut fields: Vec<(String, FieldValue)> = Vec::new();
    for pair in split_unescaped(sections[1], b',') {
        if pair.is_empty() {
            return Err(LpError::TrailingSeparator { part: "field set" });
        }
        let (k, v) = split_once_unescaped(pair, b'=')
            .ok_or(LpError::MissingSeparator { part: "field" })?;
        if k.is_empty() {
            return Err(LpError::EmptyKey { part: "field" });
        }
        let key = unescape(k);
        let value = parse_field_value(v, &key)?;
        fields.push((key, value));
    }
    if fields.is_empty() {
        return Err(LpError::MissingFields);
    }

    // Section 3: optional timestamp.
    let timestamp = match sections.get(2) {
        None => None,
        Some(raw) => {
            let raw = raw.trim();
            if raw.is_empty() {
                None
            } else {
                Some(
                    raw.parse::<i64>()
                        .map_err(|_| LpError::BadTimestamp { got: raw.to_string() })?,
                )
            }
        }
    };
    if sections.len() > 3 {
        return Err(LpError::BadTimestamp {
            got: sections[3..].join(" "),
        });
    }

    Ok(LpPoint {
        measurement,
        tags,
        fields,
        timestamp,
    })
}

/// Parse a newline-separated payload.
///
/// Blank lines and `#` comments are skipped rather than reported, because a
/// Telegraf payload routinely contains them and rejecting the whole batch over
/// one comment would be hostile. Returns the points that parsed and the errors
/// for those that did not, so a caller can ingest the good rows and report the
/// bad ones instead of losing the batch.
pub fn parse_batch(payload: &str) -> (Vec<LpPoint>, Vec<(usize, LpError)>) {
    let mut points = Vec::new();
    let mut errors = Vec::new();
    for (i, line) in payload.lines().enumerate() {
        match parse_line(line) {
            Ok(p) => points.push(p),
            // A blank line or comment is not an error worth reporting.
            Err(LpError::Empty) => continue,
            Err(e) => errors.push((i + 1, e)),
        }
    }
    (points, errors)
}

fn parse_field_value(raw: &str, key: &str) -> Result<FieldValue, LpError> {
    if let Some(body) = raw.strip_prefix('"') {
        // Quoted string: only `\"` escapes a quote, and `\\` a backslash.
        let bytes = body.as_bytes();
        let mut out = String::with_capacity(body.len());
        let mut i = 0usize;
        while i < bytes.len() {
            match bytes[i] {
                b'"' => {
                    // Must be the final character, apart from nothing.
                    if i + 1 != bytes.len() {
                        return Err(LpError::BadFieldValue {
                            key: key.to_string(),
                            value: raw.to_string(),
                        });
                    }
                    return Ok(FieldValue::Str(out));
                }
                b'\\' if i + 1 < bytes.len() => {
                    out.push(bytes[i + 1] as char);
                    i += 2;
                }
                b => {
                    out.push(b as char);
                    i += 1;
                }
            }
        }
        return Err(LpError::UnterminatedString);
    }

    let bad = || LpError::BadFieldValue {
        key: key.to_string(),
        value: raw.to_string(),
    };

    match raw {
        "t" | "T" | "true" | "True" | "TRUE" => return Ok(FieldValue::Bool(true)),
        "f" | "F" | "false" | "False" | "FALSE" => return Ok(FieldValue::Bool(false)),
        _ => {}
    }

    if let Some(body) = raw.strip_suffix('i') {
        return body
            .parse::<i64>()
            .map(FieldValue::Int)
            .map_err(|_| bad());
    }
    if let Some(body) = raw.strip_suffix('u') {
        return body
            .parse::<u64>()
            .map(FieldValue::UInt)
            .map_err(|_| bad());
    }

    raw.parse::<f64>().map(FieldValue::Float).map_err(|_| bad())
}

/// Split on `sep` bytes that are not backslash-escaped.
fn split_unescaped(s: &str, sep: u8) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2;
            continue;
        }
        if bytes[i] == sep {
            out.push(&s[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    out.push(&s[start..]);
    out
}

/// Split at the first unescaped `sep`, returning `None` if there is none.
fn split_once_unescaped(s: &str, sep: u8) -> Option<(&str, &str)> {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2;
            continue;
        }
        if bytes[i] == sep {
            return Some((&s[..i], &s[i + 1..]));
        }
        i += 1;
    }
    None
}

/// Remove line-protocol backslash escapes.
///
/// A backslash before any other character is kept literally, because the
/// protocol only defines escapes for `,`, `=`, space, `"` and `\`; silently
/// eating the backslash would corrupt Windows-style paths and regexes written
/// into a string field.
fn unescape(s: &str) -> String {
    let bytes = s.as_bytes();
    if !bytes.contains(&b'\\') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            match bytes[i + 1] {
                b',' | b'=' | b' ' | b'"' | b'\\' => {
                    out.push(bytes[i + 1] as char);
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        // Push the whole UTF-8 sequence, not one byte.
        let ch_len = utf8_len(bytes[i]);
        out.push_str(&s[i..i + ch_len]);
        i += ch_len;
    }
    out
}

fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else if b >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_canonical_example() {
        let p = parse_line("weather,location=us-midwest,season=summer temperature=82,humidity=71i 1465839830100400200")
            .unwrap();
        assert_eq!(p.measurement, "weather");
        assert_eq!(
            p.tags,
            vec![
                ("location".to_string(), "us-midwest".to_string()),
                ("season".to_string(), "summer".to_string())
            ]
        );
        assert_eq!(
            p.fields,
            vec![
                ("temperature".to_string(), FieldValue::Float(82.0)),
                ("humidity".to_string(), FieldValue::Int(71))
            ]
        );
        assert_eq!(p.timestamp, Some(1465839830100400200));
    }

    #[test]
    fn keeps_field_types_distinct() {
        let p = parse_line("m f=1,gi=1i,gs=\"1\",gb=true").unwrap();
        assert_eq!(p.fields[0].1, FieldValue::Float(1.0));
        assert_eq!(p.fields[1].1, FieldValue::Int(1));
        assert_eq!(p.fields[2].1, FieldValue::Str("1".to_string()));
        assert_eq!(p.fields[3].1, FieldValue::Bool(true));
    }

    #[test]
    fn unsigned_integers_are_kept_unsigned() {
        let p = parse_line("m v=18446744073709551615u").unwrap();
        assert_eq!(p.fields[0].1, FieldValue::UInt(u64::MAX));
    }

    #[test]
    fn scientific_notation_is_a_float() {
        let p = parse_line("m v=1.5e-3").unwrap();
        assert_eq!(p.fields[0].1, FieldValue::Float(1.5e-3));
    }

    #[test]
    fn escapes_measurement_tags_and_keys() {
        let p = parse_line("wea\\ ther,loc\\=ation=us\\ midwest temp\\ erature=1").unwrap();
        assert_eq!(p.measurement, "wea ther");
        assert_eq!(p.tags, vec![("loc=ation".to_string(), "us midwest".to_string())]);
        assert_eq!(p.fields[0].0, "temp erature");
    }

    #[test]
    fn spaces_inside_a_string_field_are_not_separators() {
        let p = parse_line("m note=\"hello world\",v=1 5").unwrap();
        assert_eq!(p.fields[0].1, FieldValue::Str("hello world".to_string()));
        assert_eq!(p.fields[1].1, FieldValue::Float(1.0));
        assert_eq!(p.timestamp, Some(5));
    }

    #[test]
    fn escaped_quote_inside_a_string_field() {
        let p = parse_line("m note=\"say \\\"hi\\\"\",v=1").unwrap();
        assert_eq!(p.fields[0].1, FieldValue::Str("say \"hi\"".to_string()));
    }

    #[test]
    fn backslash_before_an_ordinary_character_is_kept() {
        let p = parse_line("m path=\"C:\\\\Users\\\\x\",v=1").unwrap();
        assert_eq!(p.fields[0].1, FieldValue::Str("C:\\Users\\x".to_string()));
    }

    #[test]
    fn timestamp_is_optional() {
        let p = parse_line("m v=1").unwrap();
        assert_eq!(p.timestamp, None);
    }

    #[test]
    fn multi_byte_measurement_survives_unescaping() {
        let p = parse_line(r#"温度,城市=北京 value=21.5"#).unwrap();
        assert_eq!(p.measurement, "温度");
        assert_eq!(p.tags, vec![("城市".to_string(), "北京".to_string())]);
    }

    #[test]
    fn rejects_structural_mistakes() {
        assert_eq!(parse_line(""), Err(LpError::Empty));
        assert_eq!(parse_line("   "), Err(LpError::Empty));
        assert_eq!(parse_line("# a comment"), Err(LpError::Empty));
        assert_eq!(parse_line("m"), Err(LpError::MissingFields));
        assert!(matches!(
            parse_line("m,loc,bad v=1"),
            Err(LpError::MissingSeparator { part: "tag" })
        ));
        assert!(matches!(
            parse_line("m,loc= v=1"),
            Err(LpError::EmptyTagPart)
        ));
        assert_eq!(
            parse_line("m v=abc"),
            Err(LpError::BadFieldValue {
                key: "v".into(),
                value: "abc".into()
            })
        );
        // A third space-separated token is the timestamp slot, so a stray word
        // there is a bad timestamp rather than a malformed tag.
        assert_eq!(
            parse_line("m v=1 loc"),
            Err(LpError::BadTimestamp { got: "loc".into() })
        );
        assert_eq!(parse_line("m v=1 notanumber"), Err(LpError::BadTimestamp { got: "notanumber".into() }));
        assert_eq!(parse_line(r#"m v="unclosed"#), Err(LpError::UnterminatedString));
    }

    #[test]
    fn a_trailing_comma_is_rejected_rather_than_ignored() {
        // Silently dropping it would write a field the caller believes it sent.
        assert_eq!(
            parse_line("m v=1,"),
            Err(LpError::TrailingSeparator { part: "field set" })
        );
    }

    #[test]
    fn batch_skips_blanks_and_comments_but_reports_real_errors() {
        let payload = "\
# telegraf output
m v=1 10
m v=2 20
this line is broken
m v=3 30
";
        let (points, errors) = parse_batch(payload);
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].timestamp, Some(10));
        assert_eq!(points[2].timestamp, Some(30));
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, 4, "error must carry the 1-based line number");
    }

    #[test]
    fn precision_converts_between_units() {
        use tmdb_timestore::TsUnit;
        assert_eq!(Precision::Seconds.to_ts_unit(5, TsUnit::Millis), Some(5_000));
        assert_eq!(Precision::Milliseconds.to_ts_unit(5_000, TsUnit::Seconds), Some(5));
        assert_eq!(
            Precision::Nanoseconds.to_ts_unit(1_500_000_000, TsUnit::Millis),
            Some(1_500)
        );
    }

    #[test]
    fn precision_overflow_is_reported_not_wrapped() {
        use tmdb_timestore::TsUnit;
        assert_eq!(Precision::Seconds.to_ts_unit(i64::MAX, TsUnit::Nanos), None);
    }
}