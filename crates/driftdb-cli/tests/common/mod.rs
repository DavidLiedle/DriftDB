use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

/// Test helper for creating temporary databases
pub struct TestDb {
    pub dir: TempDir,
    pub path: PathBuf,
}

impl TestDb {
    /// Create a new temporary database directory
    pub fn new() -> Self {
        let dir = TempDir::new().expect("Failed to create temp dir");
        let path = dir.path().to_path_buf();
        Self { dir, path }
    }

    /// Get the database path as a string
    pub fn path_str(&self) -> &str {
        self.path.to_str().expect("Invalid path")
    }
}

/// Create a JSONL test file with sample data
pub fn create_jsonl_file(dir: &TempDir, filename: &str, records: &[&str]) -> PathBuf {
    let path = dir.path().join(filename);
    let mut file = File::create(&path).expect("Failed to create JSONL file");
    for record in records {
        writeln!(file, "{}", record).expect("Failed to write record");
    }
    path
}

/// Create a SQL file with multiple statements
pub fn create_sql_file(dir: &TempDir, filename: &str, statements: &[&str]) -> PathBuf {
    let path = dir.path().join(filename);
    let mut file = File::create(&path).expect("Failed to create SQL file");
    for stmt in statements {
        writeln!(file, "{}", stmt).expect("Failed to write statement");
    }
    path
}
