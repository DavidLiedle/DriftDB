mod common;

use assert_cmd::Command;
use predicates::prelude::*;

use common::{create_jsonl_file, create_sql_file, TestDb};

fn driftdb() -> Command {
    Command::new(env!("CARGO_BIN_EXE_driftdb"))
}

#[test]
fn test_init_creates_database() {
    let db = TestDb::new();

    driftdb()
        .arg("init")
        .arg(db.path_str())
        .assert()
        .success()
        .stdout(predicate::str::contains("Initialized DriftDB"));

    // Verify database directory was created with expected structure
    assert!(db.path.exists());
}

#[test]
fn test_sql_execute_create_table() {
    let db = TestDb::new();

    // Initialize database first
    driftdb().arg("init").arg(db.path_str()).assert().success();

    // Create a table using SQL (PRIMARY KEY must be a table constraint)
    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg("CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR, PRIMARY KEY (id))")
        .assert()
        .success();
}

#[test]
fn test_sql_insert_and_select() {
    let db = TestDb::new();

    // Initialize and create table
    driftdb().arg("init").arg(db.path_str()).assert().success();

    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg("CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR, PRIMARY KEY (id))")
        .assert()
        .success();

    // Insert a row
    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg(r#"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"#)
        .assert()
        .success();

    // Select should return the row
    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg("SELECT * FROM users")
        .assert()
        .success()
        .stdout(predicate::str::contains("Alice"));
}

#[test]
fn test_ingest_jsonl() {
    let db = TestDb::new();

    // Initialize and create table
    driftdb().arg("init").arg(db.path_str()).assert().success();

    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg(
            "CREATE TABLE orders (id INTEGER, product VARCHAR, quantity INTEGER, PRIMARY KEY (id))",
        )
        .assert()
        .success();

    // Create JSONL file
    let jsonl_path = create_jsonl_file(
        &db.dir,
        "orders.jsonl",
        &[
            r#"{"id": 1, "product": "Widget", "quantity": 10}"#,
            r#"{"id": 2, "product": "Gadget", "quantity": 5}"#,
            r#"{"id": 3, "product": "Gizmo", "quantity": 15}"#,
        ],
    );

    // Ingest the file
    driftdb()
        .arg("ingest")
        .arg("-d")
        .arg(db.path_str())
        .arg("-t")
        .arg("orders")
        .arg("-f")
        .arg(jsonl_path.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicate::str::contains("Ingested 3 rows"));
}

#[test]
fn test_select_with_where_clause() {
    let db = TestDb::new();

    // Initialize and create table
    driftdb().arg("init").arg(db.path_str()).assert().success();

    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg("CREATE TABLE products (id INTEGER, name VARCHAR, status VARCHAR, PRIMARY KEY (id))")
        .assert()
        .success();

    // Insert rows
    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg(r#"INSERT INTO products VALUES (1, 'Product A', 'active')"#)
        .assert()
        .success();

    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg(r#"INSERT INTO products VALUES (2, 'Product B', 'inactive')"#)
        .assert()
        .success();

    // Select with where clause
    driftdb()
        .arg("select")
        .arg("-d")
        .arg(db.path_str())
        .arg("-t")
        .arg("products")
        .arg("-w")
        .arg(r#"status="active""#)
        .assert()
        .success()
        .stdout(predicate::str::contains("Product A"))
        .stdout(predicate::str::contains("Product B").not());
}

#[test]
fn test_snapshot_command() {
    let db = TestDb::new();

    // Initialize and create table with data
    driftdb().arg("init").arg(db.path_str()).assert().success();

    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg("CREATE TABLE items (id INTEGER, name VARCHAR, PRIMARY KEY (id))")
        .assert()
        .success();

    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg(r#"INSERT INTO items VALUES (1, 'Item One')"#)
        .assert()
        .success();

    // Create snapshot
    driftdb()
        .arg("snapshot")
        .arg("-d")
        .arg(db.path_str())
        .arg("-t")
        .arg("items")
        .assert()
        .success();
}

#[test]
fn test_doctor_command() {
    let db = TestDb::new();

    // Initialize database
    driftdb().arg("init").arg(db.path_str()).assert().success();

    // Doctor should run without error on new database
    driftdb()
        .arg("doctor")
        .arg("-d")
        .arg(db.path_str())
        .assert()
        .success();
}

#[test]
fn test_sql_file_execution() {
    let db = TestDb::new();

    // Initialize database
    driftdb().arg("init").arg(db.path_str()).assert().success();

    // Create SQL file with multiple statements
    let sql_path = create_sql_file(
        &db.dir,
        "setup.sql",
        &[
            "CREATE TABLE logs (id INTEGER, message VARCHAR, level VARCHAR, PRIMARY KEY (id))",
            r#"INSERT INTO logs VALUES (1, 'System started', 'info')"#,
            r#"INSERT INTO logs VALUES (2, 'Connection failed', 'error')"#,
        ],
    );

    // Execute the SQL file
    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-f")
        .arg(sql_path.to_str().unwrap())
        .assert()
        .success();

    // Verify data was inserted
    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg("SELECT * FROM logs")
        .assert()
        .success()
        .stdout(predicate::str::contains("System started"));
}

#[test]
fn test_error_missing_table() {
    let db = TestDb::new();

    // Initialize database
    driftdb().arg("init").arg(db.path_str()).assert().success();

    // Try to select from non-existent table
    driftdb()
        .arg("select")
        .arg("-d")
        .arg(db.path_str())
        .arg("-t")
        .arg("nonexistent")
        .assert()
        .failure();
}

#[test]
fn test_error_invalid_sql() {
    let db = TestDb::new();

    // Initialize database
    driftdb().arg("init").arg(db.path_str()).assert().success();

    // Try to execute invalid SQL
    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg("THIS IS NOT VALID SQL")
        .assert()
        .failure();
}

#[test]
fn test_select_with_limit() {
    let db = TestDb::new();

    // Initialize and create table
    driftdb().arg("init").arg(db.path_str()).assert().success();

    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg("CREATE TABLE numbers (id INTEGER, value INTEGER, PRIMARY KEY (id))")
        .assert()
        .success();

    // Insert multiple rows
    for i in 1..=5 {
        driftdb()
            .arg("sql")
            .arg("-d")
            .arg(db.path_str())
            .arg("-e")
            .arg(format!(r#"INSERT INTO numbers VALUES ({}, {})"#, i, i * 10))
            .assert()
            .success();
    }

    // Select with limit
    driftdb()
        .arg("select")
        .arg("-d")
        .arg(db.path_str())
        .arg("-t")
        .arg("numbers")
        .arg("-l")
        .arg("2")
        .assert()
        .success();
}

#[test]
fn test_select_json_output() {
    let db = TestDb::new();

    // Initialize and create table
    driftdb().arg("init").arg(db.path_str()).assert().success();

    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg("CREATE TABLE data (id INTEGER, name VARCHAR, PRIMARY KEY (id))")
        .assert()
        .success();

    driftdb()
        .arg("sql")
        .arg("-d")
        .arg(db.path_str())
        .arg("-e")
        .arg(r#"INSERT INTO data VALUES (1, 'Test')"#)
        .assert()
        .success();

    // Select with --json flag
    driftdb()
        .arg("select")
        .arg("-d")
        .arg(db.path_str())
        .arg("-t")
        .arg("data")
        .arg("--json")
        .assert()
        .success()
        .stdout(predicate::str::contains("["))
        .stdout(predicate::str::contains("]"));
}
