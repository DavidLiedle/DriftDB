use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use driftdb_core::{Engine, Query, QueryResult};
use serde_json::json;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use time::OffsetDateTime;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "driftdb")]
#[command(about = "DriftDB - Append-only database with time-travel queries")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new DriftDB database
    Init {
        /// Database directory path
        path: PathBuf,
    },
    /// Execute SQL queries
    Sql {
        /// Database directory path
        #[arg(short, long)]
        data: PathBuf,
        /// SQL query to execute
        #[arg(short, long, conflicts_with = "file")]
        execute: Option<String>,
        /// SQL file to execute
        #[arg(short, long, conflicts_with = "execute")]
        file: Option<PathBuf>,
    },
    /// Ingest data from JSONL file
    Ingest {
        /// Database directory path
        #[arg(short, long)]
        data: PathBuf,
        /// Table name
        #[arg(short, long)]
        table: String,
        /// JSONL file to ingest
        #[arg(short, long)]
        file: PathBuf,
    },
    /// Select data from a table
    Select {
        /// Database directory path
        #[arg(short, long)]
        data: PathBuf,
        /// Table name
        #[arg(short, long)]
        table: String,
        /// WHERE condition (e.g., 'status="paid"')
        #[arg(short, long)]
        r#where: Option<String>,
        /// AS OF timestamp or sequence
        #[arg(long)]
        as_of: Option<String>,
        /// Limit number of results
        #[arg(short, long)]
        limit: Option<usize>,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    /// Show drift history for a row
    Drift {
        /// Database directory path
        #[arg(short, long)]
        data: PathBuf,
        /// Table name
        #[arg(short, long)]
        table: String,
        /// Primary key value
        #[arg(short, long)]
        key: String,
    },
    /// Create a snapshot
    Snapshot {
        /// Database directory path
        #[arg(short, long)]
        data: PathBuf,
        /// Table name
        #[arg(short, long)]
        table: String,
    },
    /// Compact a table
    Compact {
        /// Database directory path
        #[arg(short, long)]
        data: PathBuf,
        /// Table name
        #[arg(short, long)]
        table: String,
    },
    /// Check and repair database integrity
    Doctor {
        /// Database directory path
        #[arg(short, long)]
        data: PathBuf,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("driftdb=info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path } => {
            Engine::init(&path)?;
            println!("Initialized DriftDB at {}", path.display());
        }
        Commands::Sql { data, execute, file } => {
            let mut engine = Engine::open(&data)
                .context("Failed to open database")?;

            let queries = if let Some(query) = execute {
                vec![query]
            } else if let Some(file) = file {
                let content = fs::read_to_string(&file)
                    .context("Failed to read SQL file")?;
                content
                    .lines()
                    .filter(|line| !line.trim().is_empty() && !line.trim().starts_with("--"))
                    .map(String::from)
                    .collect()
            } else {
                return Err(anyhow::anyhow!("Must provide either -e or -f"));
            };

            for query_str in queries {
                let query = driftdb_core::query::parse_driftql(&query_str)
                    .context("Failed to parse query")?;
                let result = engine.execute_query(query)
                    .context("Failed to execute query")?;

                match result {
                    QueryResult::Success { message } => println!("{}", message),
                    QueryResult::Rows { data } => {
                        for row in data {
                            println!("{}", serde_json::to_string_pretty(&row)?);
                        }
                    }
                    QueryResult::DriftHistory { events } => {
                        for event in events {
                            println!("{}", serde_json::to_string_pretty(&event)?);
                        }
                    }
                    QueryResult::Error { message } => eprintln!("Error: {}", message),
                }
            }
        }
        Commands::Ingest { data, table, file } => {
            let mut engine = Engine::open(&data)
                .context("Failed to open database")?;

            let file = fs::File::open(&file)
                .context("Failed to open JSONL file")?;
            let reader = BufReader::new(file);

            let mut count = 0;
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                let data: serde_json::Value = serde_json::from_str(&line)
                    .context("Failed to parse JSON")?;

                let query = Query::Insert {
                    table: table.clone(),
                    data,
                };

                engine.execute_query(query)
                    .context("Failed to insert row")?;
                count += 1;
            }

            println!("Ingested {} rows into table '{}'", count, table);
        }
        Commands::Select {
            data,
            table,
            r#where,
            as_of,
            limit,
            json: output_json,
        } => {
            let engine = Engine::open(&data)
                .context("Failed to open database")?;

            let conditions = if let Some(where_clause) = r#where {
                parse_where_clause(&where_clause)?
            } else {
                vec![]
            };

            let as_of = parse_as_of(as_of.as_deref())?;

            let query = Query::Select {
                table: table.clone(),
                conditions,
                as_of,
                limit,
            };

            let mut engine_mut = engine;
            let result = engine_mut.execute_query(query)
                .context("Failed to execute select")?;

            match result {
                QueryResult::Rows { data } => {
                    if output_json {
                        println!("{}", serde_json::to_string_pretty(&data)?);
                    } else {
                        for row in data {
                            println!("{}", serde_json::to_string_pretty(&row)?);
                        }
                    }
                }
                _ => {}
            }
        }
        Commands::Drift { data, table, key } => {
            let mut engine = Engine::open(&data)
                .context("Failed to open database")?;

            let primary_key = parse_key_value(&key)?;

            let query = Query::ShowDrift {
                table,
                primary_key,
            };

            let result = engine.execute_query(query)
                .context("Failed to get drift history")?;

            match result {
                QueryResult::DriftHistory { events } => {
                    for event in events {
                        println!("{}", serde_json::to_string_pretty(&event)?);
                    }
                }
                _ => {}
            }
        }
        Commands::Snapshot { data, table } => {
            let mut engine = Engine::open(&data)
                .context("Failed to open database")?;

            let query = Query::Snapshot { table: table.clone() };
            let result = engine.execute_query(query)
                .context("Failed to create snapshot")?;

            match result {
                QueryResult::Success { message } => println!("{}", message),
                _ => {}
            }
        }
        Commands::Compact { data, table } => {
            let mut engine = Engine::open(&data)
                .context("Failed to open database")?;

            let query = Query::Compact { table: table.clone() };
            let result = engine.execute_query(query)
                .context("Failed to compact table")?;

            match result {
                QueryResult::Success { message } => println!("{}", message),
                _ => {}
            }
        }
        Commands::Doctor { data } => {
            let engine = Engine::open(&data)
                .context("Failed to open database")?;

            let report = engine.doctor()
                .context("Failed to run doctor")?;

            for line in report {
                println!("{}", line);
            }
        }
    }

    Ok(())
}

fn parse_where_clause(clause: &str) -> Result<Vec<driftdb_core::query::WhereCondition>> {
    let mut conditions = Vec::new();

    for part in clause.split(" AND ") {
        if let Some((column, value)) = part.split_once('=') {
            let column = column.trim().to_string();
            let value_str = value.trim().trim_matches('"');
            let value = if let Ok(num) = value_str.parse::<f64>() {
                json!(num)
            } else {
                json!(value_str)
            };

            conditions.push(driftdb_core::query::WhereCondition {
                column,
                operator: "=".to_string(),
                value
            });
        }
    }

    Ok(conditions)
}

fn parse_as_of(as_of: Option<&str>) -> Result<Option<driftdb_core::query::AsOf>> {
    match as_of {
        None => Ok(None),
        Some("@now") => Ok(Some(driftdb_core::query::AsOf::Now)),
        Some(s) if s.starts_with("@seq:") => {
            let seq = s[5..].parse::<u64>()
                .context("Invalid sequence number")?;
            Ok(Some(driftdb_core::query::AsOf::Sequence(seq)))
        }
        Some(s) => {
            let timestamp = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                .context("Invalid timestamp format")?;
            Ok(Some(driftdb_core::query::AsOf::Timestamp(timestamp)))
        }
    }
}

fn parse_key_value(key: &str) -> Result<serde_json::Value> {
    if let Ok(num) = key.parse::<f64>() {
        Ok(json!(num))
    } else {
        Ok(json!(key))
    }
}
