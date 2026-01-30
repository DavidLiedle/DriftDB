use crate::errors::security_error;
use anyhow::{anyhow, Result};
use tracing::{debug, warn};

/// SQL validation module to prevent injection attacks
/// Uses a smarter approach that detects actual injection patterns
/// rather than blocking legitimate SQL syntax
pub struct SqlValidator {
    /// Maximum query length to prevent DoS
    max_query_length: usize,
}

impl SqlValidator {
    pub fn new() -> Self {
        Self {
            max_query_length: 100_000, // 100KB max query
        }
    }

    /// Validates a SQL query for safety before execution
    /// Uses pattern detection to identify likely injection attempts
    pub fn validate_query(&self, sql: &str) -> Result<()> {
        self.validate_query_with_context(sql, "unknown")
    }

    /// Validates a SQL query with client context for better error reporting
    pub fn validate_query_with_context(&self, sql: &str, client_addr: &str) -> Result<()> {
        debug!("Validating SQL query: {}", sql);

        // Check query length
        if sql.len() > self.max_query_length {
            warn!("Query exceeds maximum length: {} bytes", sql.len());
            return Err(anyhow!(
                "Query too long (max {} bytes)",
                self.max_query_length
            ));
        }

        let sql_upper = sql.to_uppercase();

        // Explicitly allow transaction commands - these are safe standalone commands
        let safe_commands = ["BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION", "END"];
        let trimmed = sql_upper.trim();
        for cmd in safe_commands {
            if trimmed == cmd {
                debug!("Allowing safe transaction command: {}", cmd);
                return Ok(());
            }
        }

        // Detect common injection patterns
        if self.detect_comment_injection(&sql_upper) {
            let error = security_error("SQL comment injection detected", client_addr, Some(sql));
            error.log();
            return Err(anyhow!("SQL injection attempt detected: comment injection"));
        }

        if self.detect_stacked_queries(&sql_upper) {
            warn!("Stacked queries injection detected");
            return Err(anyhow!("SQL injection attempt detected: stacked queries"));
        }

        if self.detect_union_injection(&sql_upper) {
            warn!("UNION injection detected");
            return Err(anyhow!("SQL injection attempt detected: UNION injection"));
        }

        if self.detect_tautology_injection(&sql_upper) {
            warn!("Tautology injection detected");
            return Err(anyhow!("SQL injection attempt detected: tautology"));
        }

        if self.detect_system_command_injection(&sql_upper) {
            warn!("System command injection detected");
            return Err(anyhow!("SQL injection attempt detected: system commands"));
        }

        if self.detect_timing_attack(&sql_upper) {
            warn!("Timing attack detected");
            return Err(anyhow!("SQL injection attempt detected: timing attack"));
        }

        // Check for null bytes
        if sql.contains('\0') {
            warn!("Query contains null bytes");
            return Err(anyhow!("Query contains null bytes"));
        }

        debug!("SQL query validation passed");
        Ok(())
    }

    /// Detect comment-based injection attempts
    fn detect_comment_injection(&self, sql: &str) -> bool {
        // Look for suspicious comment patterns that terminate queries
        let patterns = [
            "'; --",
            "\"; --",
            "') --",
            "\") --",
            "'; #",
            "\"; #",
            " OR 1=1--",
            " OR '1'='1'--",
        ];

        for pattern in patterns {
            if sql.contains(pattern) {
                return true;
            }
        }

        // Check for comment after DROP, DELETE, UPDATE without WHERE
        if (sql.contains("DROP ") || sql.contains("DELETE FROM")) && sql.contains("--") {
            // More suspicious if there's a comment after dangerous operations
            let parts: Vec<&str> = sql.split("--").collect();
            if parts.len() > 1 && !parts[0].contains("WHERE") {
                return true;
            }
        }

        false
    }

    /// Detect stacked query injection (multiple queries separated by semicolon)
    fn detect_stacked_queries(&self, sql: &str) -> bool {
        // Look for patterns like '; DROP TABLE
        let dangerous_after_semicolon = [
            "; DROP ",
            "; DELETE ",
            "; INSERT ",
            "; UPDATE ",
            "; CREATE ",
            "; ALTER ",
            "; EXEC",
            "; TRUNCATE",
        ];

        for pattern in dangerous_after_semicolon {
            if sql.contains(pattern) {
                return true;
            }
        }

        // Also check for quotes followed by semicolon and dangerous commands
        let quote_patterns = ["'; DROP", "'; DELETE", "\"; DROP", "\"; DELETE"];

        for pattern in quote_patterns {
            if sql.contains(pattern) {
                return true;
            }
        }

        false
    }

    /// Detect UNION-based injection
    fn detect_union_injection(&self, sql: &str) -> bool {
        // UNION SELECT is almost always an injection when combined with certain patterns
        if sql.contains("UNION") {
            // Check for common injection patterns with UNION
            let suspicious_patterns = [
                "UNION ALL SELECT",
                "UNION SELECT", // Added general UNION SELECT
                "UNION ALL",
                "UNION DISTINCT",
                " UNION ", // UNION with spaces (common in injections)
                "'UNION",
                "\"UNION",
                ")UNION",
                "UNION(",
                "UNION/*", // UNION with comment
                "UNION--", // UNION with comment
                "UNION#",  // UNION with comment
                "UNION SELECT NULL",
                "UNION SELECT 1",
                "UNION SELECT @@VERSION",
                "UNION SELECT USER()",
                "UNION SELECT DATABASE()",
                "UNION SELECT SCHEMA_NAME",
                "UNION SELECT PASSWORD",    // Common target
                "UNION SELECT TABLE_NAME",  // Information schema access
                "UNION SELECT COLUMN_NAME", // Information schema access
            ];

            for pattern in suspicious_patterns {
                if sql.contains(pattern) {
                    return true;
                }
            }

            // Check if UNION appears after a quote (likely injection)
            if sql.contains("' UNION") || sql.contains("\" UNION") {
                return true;
            }

            // Check for UNION in subqueries (less common but still dangerous)
            if sql.contains("(SELECT") && sql.contains("UNION") {
                return true;
            }

            // If UNION is used with FROM clause referencing different tables
            // This is a common injection pattern
            if sql.contains("FROM") && sql.contains("UNION") {
                // Check if it's trying to access system tables
                let system_tables = ["INFORMATION_SCHEMA", "MYSQL", "SYS", "PG_", "SQLITE_"];
                for table in system_tables {
                    if sql.contains(table) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Detect tautology-based injection (always true conditions)
    fn detect_tautology_injection(&self, sql: &str) -> bool {
        // Common tautology patterns
        let patterns = [
            " OR 1=1",
            " OR '1'='1'",
            " OR \"1\"=\"1\"",
            " OR 'A'='A'",
            " OR ''=''",
            " OR 1=1 --",
            " OR TRUE",
            "WHERE 1=1 AND",
            "WHERE '1'='1' AND",
        ];

        for pattern in patterns {
            if sql.contains(pattern) {
                // Make sure it's not in a string literal
                // This is a simple check - could be made more sophisticated
                let before_pattern = sql.split(pattern).next().unwrap_or("");
                let single_quotes = before_pattern.matches('\'').count();
                let double_quotes = before_pattern.matches('"').count();

                // If quotes are balanced, it's likely not in a string
                if single_quotes % 2 == 0 && double_quotes % 2 == 0 {
                    return true;
                }
            }
        }

        false
    }

    /// Detect attempts to execute system commands
    fn detect_system_command_injection(&self, sql: &str) -> bool {
        let dangerous_functions = [
            "XP_CMDSHELL",
            "SP_EXECUTESQL",
            "EXEC(",
            "EXECUTE(",
            "LOAD_FILE",
            "INTO OUTFILE",
            "INTO DUMPFILE",
            "../",
            "..\\",
            "/ETC/PASSWD",
            "C:\\",
        ];

        for func in dangerous_functions {
            if sql.contains(func) {
                return true;
            }
        }

        false
    }

    /// Detect timing-based blind SQL injection attempts
    fn detect_timing_attack(&self, sql: &str) -> bool {
        let timing_functions = [
            "SLEEP(",
            "WAITFOR DELAY",
            "BENCHMARK(",
            "PG_SLEEP(",
            "DBMS_LOCK.SLEEP",
        ];

        for func in timing_functions {
            if sql.contains(func) {
                return true;
            }
        }

        false
    }
}

impl Default for SqlValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_queries() {
        let validator = SqlValidator::new();

        let safe_queries = vec![
            "SELECT * FROM users WHERE id = $1",
            "INSERT INTO products (name, price) VALUES ($1, $2)",
            "UPDATE users SET name = $1 WHERE id = $2",
            "DELETE FROM products WHERE id = $1",
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
            "SELECT COUNT(*) FROM orders",
            "SELECT * FROM users WHERE age > 25",
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "CREATE INDEX idx_age ON users (age)",
            // Should allow legitimate use of semicolons in CREATE TABLE
            "CREATE TABLE test (id INT PRIMARY KEY, name TEXT); CREATE INDEX idx_name ON test(name)",
            // Should allow parentheses and normal SQL syntax
            "SELECT * FROM users WHERE (age > 18 AND city = 'NYC') OR status = 'active'",
        ];

        for query in safe_queries {
            assert!(
                validator.validate_query(query).is_ok(),
                "Safe query should pass: {}",
                query
            );
        }
    }

    #[test]
    fn test_injection_attempts() {
        let validator = SqlValidator::new();

        let malicious_queries = vec![
            "SELECT * FROM users WHERE id = 1'; DROP TABLE users; --",
            "SELECT * FROM users WHERE name = 'admin' OR '1'='1'",
            "SELECT * FROM users; DELETE FROM users WHERE 1=1; --",
            "SELECT * FROM users UNION SELECT password FROM admin",
            "'; INSERT INTO users (name) VALUES ('hacker'); --",
            "SELECT load_file('/etc/passwd')",
            "SELECT * FROM users WHERE name = 'test' AND sleep(10)",
            "SELECT * FROM users WHERE id = 1 OR 1=1 --",
            "admin' --",
            "' OR '1'='1",
            "1' UNION ALL SELECT NULL,NULL,NULL--",
            "'; exec xp_cmdshell 'dir' --",
        ];

        for query in malicious_queries {
            assert!(
                validator.validate_query(query).is_err(),
                "Malicious query should be blocked: {}",
                query
            );
        }
    }

    #[test]
    fn test_length_limit() {
        let validator = SqlValidator::new();
        let long_query = "SELECT ".repeat(50000) + " * FROM users";

        assert!(validator.validate_query(&long_query).is_err());
    }
}
