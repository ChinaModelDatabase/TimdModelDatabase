/// SQL parser + dialect normalization for tmDB.
///
/// Supports both PostgreSQL and MySQL dialects, normalizing to a unified AST.

use sqlparser::dialect::{PostgreSqlDialect, MySqlDialect};
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;

/// Dialect type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    MySql,
    Auto,
}

/// Parse SQL from the given dialect
pub fn parse_sql(sql: &str, dialect: Dialect) -> anyhow::Result<Vec<Statement>> {
    let dialect_impl: Box<dyn sqlparser::dialect::Dialect> = match dialect {
        Dialect::Postgres => Box::new(PostgreSqlDialect {}),
        Dialect::MySql => Box::new(MySqlDialect {}),
        Dialect::Auto => {
            // Auto-detect: simple heuristic
            if sql.contains('`') || sql.contains("ENGINE=") {
                Box::new(MySqlDialect {})
            } else {
                Box::new(PostgreSqlDialect {})
            }
        }
    };

    let statements = Parser::parse_sql(&*dialect_impl, sql)
        .map_err(|e| anyhow::anyhow!("parse error: {}", e))?;

    Ok(statements)
}

/// Normalize a MySQL statement to PostgreSQL syntax (simplified)
pub fn mysql_to_postgres(sql: &str) -> String {
    let mut result = sql.to_string();

    // ENGINE=tmdb -> remove
    result = result.replace(" ENGINE=tmdb", "");

    // AUTO_INCREMENT -> GENERATED ALWAYS AS IDENTITY (simplified)
    result = result.replace("AUTO_INCREMENT", "GENERATED ALWAYS AS IDENTITY");

    // backticks to double quotes
    result = result.replace('`', "\"");

    result
}

/// Normalize a PostgreSQL statement to MySQL syntax (simplified)
pub fn postgres_to_mysql(sql: &str) -> String {
    let mut result = sql.to_string();

    // GENERATED ALWAYS AS IDENTITY -> AUTO_INCREMENT
    result = result.replace("GENERATED ALWAYS AS IDENTITY", "AUTO_INCREMENT");

    // double quotes to backticks
    result = result.replace('"', "`");

    result
}

/// Detect dialect from SQL content
pub fn detect_dialect(sql: &str) -> Dialect {
    if sql.contains('`') || sql.contains("ENGINE=") || sql.contains("AUTO_INCREMENT") {
        Dialect::MySql
    } else {
        Dialect::Postgres
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_postgres() {
        let sql = "SELECT * FROM dev_power WHERE ts > '2026-01-01'";
        let stmts = parse_sql(sql, Dialect::Postgres).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_mysql() {
        let sql = "SELECT * FROM `dev_power` WHERE `ts` > '2026-01-01'";
        let stmts = parse_sql(sql, Dialect::MySql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_auto_detect() {
        assert_eq!(detect_dialect("SELECT * FROM `t`"), Dialect::MySql);
        assert_eq!(detect_dialect("SELECT * FROM \"t\""), Dialect::Postgres);
    }
}
