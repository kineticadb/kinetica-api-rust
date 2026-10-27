//! Parsing of `INSERT INTO ... VALUES ...` statements.
//!
//! Used by `BatchingSqlClient` and `sqlx_support::BatchingSqlxClient` to turn SQL
//! INSERTs into `GenericRecord`s that are routed through `BulkInserter`.

use crate::sql::{SqlError, SqlOpResult};

/// Parse a SQL value string into a RecordValue based on the column type.
pub fn parse_sql_value_to_record_value(value_str: &str, column: &crate::Column) -> Result<crate::RecordValue, String> {
    let trimmed = value_str.trim();

    // Handle NULL
    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(crate::RecordValue::Null);
    }

    // Handle quoted strings - strip quotes and unescape
    let unquoted = if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        let s = &trimmed[1..trimmed.len() - 1];
        // Unescape doubled quotes
        s.replace("''", "'").replace("\"\"", "\"")
    } else {
        trimmed.to_string()
    };

    // Handle boolean literals
    if unquoted.eq_ignore_ascii_case("true") {
        return Ok(crate::RecordValue::Int(1));
    }
    if unquoted.eq_ignore_ascii_case("false") {
        return Ok(crate::RecordValue::Int(0));
    }

    use crate::ColumnBaseType;

    match column.base_type() {
        ColumnBaseType::Integer => {
            let v = unquoted.parse::<i32>().map_err(|e| e.to_string())?;
            Ok(crate::RecordValue::Int(v))
        }
        ColumnBaseType::Long => {
            let v = unquoted.parse::<i64>().map_err(|e| e.to_string())?;
            Ok(crate::RecordValue::Long(v))
        }
        ColumnBaseType::Float => {
            let v = unquoted.parse::<f32>().map_err(|e| e.to_string())?;
            Ok(crate::RecordValue::Float(v))
        }
        ColumnBaseType::Double => {
            let v = unquoted.parse::<f64>().map_err(|e| e.to_string())?;
            Ok(crate::RecordValue::Double(v))
        }
        ColumnBaseType::String => {
            Ok(crate::RecordValue::String(unquoted))
        }
        ColumnBaseType::Bytes => {
            // Parse hex string to bytes (e.g., "48454C4C4F" -> b"HELLO")
            // If not valid hex, treat as raw bytes from string
            if unquoted.chars().all(|c| c.is_ascii_hexdigit()) && unquoted.len() % 2 == 0 {
                let bytes: Result<Vec<u8>, _> = (0..unquoted.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&unquoted[i..i+2], 16))
                    .collect();
                match bytes {
                    Ok(b) => Ok(crate::RecordValue::Bytes(b)),
                    Err(_) => Ok(crate::RecordValue::Bytes(unquoted.into_bytes())),
                }
            } else {
                Ok(crate::RecordValue::Bytes(unquoted.into_bytes()))
            }
        }
    }
}

// ============================================================================
// INSERT Statement Parsing
// ============================================================================

/// Parsed INSERT statement components
#[derive(Debug)]
pub struct ParsedInsert {
    pub table_name: String,
    pub columns: Vec<String>,
    pub value_rows: Vec<Vec<String>>,
}

/// Parse an INSERT statement into its components.
///
/// Supports: INSERT INTO table (col1, col2, ...) VALUES (v1, v2, ...), (v1, v2, ...), ...
pub fn parse_insert_statement(sql: &str) -> SqlOpResult<ParsedInsert> {
    let sql = sql.trim();

    // Check for INSERT INTO
    if !sql.to_uppercase().starts_with("INSERT INTO") {
        return Err(SqlError::Parse("Expected INSERT INTO statement".into()));
    }

    // Find table name
    let after_into = &sql[11..].trim_start(); // Skip "INSERT INTO"
    let table_end = after_into.find(|c: char| c == '(' || c.is_whitespace())
        .ok_or_else(|| SqlError::Parse("Could not find table name".into()))?;
    let table_name = after_into[..table_end].trim().to_string();

    // Find columns (between first parentheses)
    let cols_start = sql.find('(')
        .ok_or_else(|| SqlError::Parse("Could not find column list".into()))?;
    let cols_end = sql[cols_start..].find(')')
        .ok_or_else(|| SqlError::Parse("Could not find end of column list".into()))?
        + cols_start;

    let cols_str = &sql[cols_start + 1..cols_end];
    let columns: Vec<String> = cols_str
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    if columns.is_empty() {
        return Err(SqlError::Parse("No columns specified".into()));
    }

    // Find VALUES keyword
    let after_cols = &sql[cols_end + 1..];
    let values_pos = after_cols.to_uppercase().find("VALUES")
        .ok_or_else(|| SqlError::Parse("Could not find VALUES keyword".into()))?;
    let values_start = cols_end + 1 + values_pos + 6; // Skip "VALUES"

    // Parse value rows
    let values_str = &sql[values_start..];
    let value_rows = parse_value_rows(values_str, columns.len())?;

    if value_rows.is_empty() {
        return Err(SqlError::Parse("No values specified".into()));
    }

    Ok(ParsedInsert {
        table_name,
        columns,
        value_rows,
    })
}

/// Parse the VALUES portion of an INSERT statement.
///
/// Handles: (v1, v2, ...), (v1, v2, ...), ...
fn parse_value_rows(values_str: &str, expected_cols: usize) -> SqlOpResult<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    let mut current_row = Vec::new();
    let mut current_value = String::new();
    let mut in_string = false;
    let mut string_char = ' ';
    let mut paren_depth = 0;

    for ch in values_str.chars() {
        match ch {
            '\'' | '"' if !in_string => {
                in_string = true;
                string_char = ch;
                current_value.push(ch);
            }
            c if in_string && c == string_char => {
                current_value.push(ch);
                in_string = false;
            }
            '(' if !in_string => {
                paren_depth += 1;
                if paren_depth > 1 {
                    current_value.push(ch);
                }
            }
            ')' if !in_string => {
                paren_depth -= 1;
                if paren_depth == 0 {
                    // End of a value row
                    let val = current_value.trim().to_string();
                    if !val.is_empty() {
                        current_row.push(val);
                    }
                    current_value.clear();

                    if current_row.len() != expected_cols {
                        return Err(SqlError::Parse(format!(
                            "Expected {} values but got {}",
                            expected_cols, current_row.len()
                        )));
                    }

                    rows.push(std::mem::take(&mut current_row));
                } else if paren_depth > 0 {
                    current_value.push(ch);
                }
            }
            ',' if !in_string && paren_depth == 1 => {
                // Value separator within a row
                current_row.push(current_value.trim().to_string());
                current_value.clear();
            }
            ',' if !in_string && paren_depth == 0 => {
                // Row separator - ignore
            }
            _ if paren_depth > 0 || in_string => {
                current_value.push(ch);
            }
            _ => {
                // Whitespace outside parens - ignore
            }
        }
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_insert_single_row() {
        let parsed = parse_insert_statement("INSERT INTO t (id, name) VALUES (1, 'Alice')").unwrap();
        assert_eq!(parsed.table_name, "t");
        assert_eq!(parsed.columns, vec!["id", "name"]);
        assert_eq!(parsed.value_rows, vec![vec!["1", "'Alice'"]]);
    }

    #[test]
    fn test_parse_insert_multiple_rows() {
        let parsed = parse_insert_statement(
            "insert into ki_home.t(id,name) values (1, 'a'), (2, 'b'),(3,'c')",
        ).unwrap();
        assert_eq!(parsed.table_name, "ki_home.t");
        assert_eq!(parsed.value_rows.len(), 3);
        assert_eq!(parsed.value_rows[2], vec!["3", "'c'"]);
    }

    #[test]
    fn test_parse_insert_quoted_separators() {
        let parsed = parse_insert_statement("INSERT INTO t (a, b) VALUES ('x, (y)', 2)").unwrap();
        assert_eq!(parsed.value_rows, vec![vec!["'x, (y)'", "2"]]);
    }

    #[test]
    fn test_parse_insert_value_count_mismatch() {
        let err = parse_insert_statement("INSERT INTO t (a, b) VALUES (1)").unwrap_err();
        assert!(matches!(err, SqlError::Parse(_)));
    }

    #[test]
    fn test_parse_insert_rejects_non_insert() {
        assert!(parse_insert_statement("SELECT * FROM t").is_err());
        assert!(parse_insert_statement("INSERT INTO t (a) SELECT 1").is_err());
    }
}
