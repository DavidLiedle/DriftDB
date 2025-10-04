use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use driftdb_core::{Engine, Query, QueryResult};
use serde_json::json;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use time::OffsetDateTime;
use tracing_subscriber::EnvFilter;

mod backup;

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
    /// Analyze tables and update optimizer statistics
    Analyze {
        /// Database directory path
        #[arg(short, long)]
        data: PathBuf,
        /// Table name (optional, analyzes all tables if not specified)
        #[arg(short, long)]
        table: Option<String>,
    },
    /// Backup and restore operations
    Backup {
        #[command(subcommand)]
        command: backup::BackupCommands,
    },
    /// Enable query performance optimization
    Optimize {
        /// Database directory path
        #[arg(short, long)]
        data: PathBuf,
        /// Enable or disable optimization
        #[arg(long)]
        enable: bool,
        /// Show optimization statistics
        #[arg(long)]
        stats: bool,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("driftdb=info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path } => {
            Engine::init(&path)?;
            println!("Initialized DriftDB at {}", path.display());
        }
        Commands::Sql {
            data,
            execute,
            file,
        } => {
            let mut engine = Engine::open(&data).context("Failed to open database")?;

            let queries = if let Some(query) = execute {
                vec![query]
            } else if let Some(file) = file {
                let content = fs::read_to_string(&file).context("Failed to read SQL file")?;
                content
                    .lines()
                    .filter(|line| !line.trim().is_empty() && !line.trim().starts_with("--"))
                    .map(String::from)
                    .collect()
            } else {
                return Err(anyhow::anyhow!("Must provide either -e or -f"));
            };

            for query_str in queries {
                // Execute all queries as SQL - 100% SQL compatibility
                let result = driftdb_core::sql_bridge::execute_sql(&mut engine, &query_str)
                    .context("Failed to execute SQL query")?;

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
            let mut engine = Engine::open(&data).context("Failed to open database")?;

            let file = fs::File::open(&file).context("Failed to open JSONL file")?;
            let reader = BufReader::new(file);

            let mut count = 0;
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                let data: serde_json::Value =
                    serde_json::from_str(&line).context("Failed to parse JSON")?;

                let query = Query::Insert {
                    table: table.clone(),
                    data,
                };

                engine
                    .execute_query(query)
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
            let engine = Engine::open(&data).context("Failed to open database")?;

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
            let result = engine_mut
                .execute_query(query)
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
            let mut engine = Engine::open(&data).context("Failed to open database")?;

            let primary_key = parse_key_value(&key)?;

            let query = Query::ShowDrift { table, primary_key };

            let result = engine
                .execute_query(query)
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
            let mut engine = Engine::open(&data).context("Failed to open database")?;

            let query = Query::Snapshot {
                table: table.clone(),
            };
            let result = engine
                .execute_query(query)
                .context("Failed to create snapshot")?;

            match result {
                QueryResult::Success { message } => println!("{}", message),
                _ => {}
            }
        }
        Commands::Compact { data, table } => {
            let mut engine = Engine::open(&data).context("Failed to open database")?;

            let query = Query::Compact {
                table: table.clone(),
            };
            let result = engine
                .execute_query(query)
                .context("Failed to compact table")?;

            match result {
                QueryResult::Success { message } => println!("{}", message),
                _ => {}
            }
        }
        Commands::Doctor { data } => {
            let engine = Engine::open(&data).context("Failed to open database")?;

            let report = engine.doctor().context("Failed to run doctor")?;

            for line in report {
                println!("{}", line);
            }
        }
        Commands::Analyze { data, table } => {
            let engine = Engine::open(&data).context("Failed to open database")?;

            // Create optimizer to store the statistics
            let optimizer = driftdb_core::optimizer::QueryOptimizer::new();

            if let Some(table_name) = table {
                // Analyze specific table
                println!("Analyzing table '{}'...", table_name);
                let stats = engine
                    .collect_table_statistics(&table_name)
                    .context(format!(
                        "Failed to collect statistics for table '{}'",
                        table_name
                    ))?;

                println!("Table: {}", stats.table_name);
                println!("  Rows: {}", stats.row_count);
                println!("  Average row size: {} bytes", stats.avg_row_size);
                println!("  Total size: {} bytes", stats.total_size_bytes);
                println!("  Columns analyzed: {}", stats.column_stats.len());
                println!("  Indexes: {}", stats.index_stats.len());

                for (col_name, col_stats) in &stats.column_stats {
                    println!("  Column '{}':", col_name);
                    println!("    Distinct values: {}", col_stats.distinct_values);
                    println!("    Null count: {}", col_stats.null_count);
                    if col_stats.histogram.is_some() {
                        println!("    Histogram: ✓");
                    }
                }

                optimizer.update_statistics(&table_name, stats);
                println!("✓ Statistics updated for table '{}'", table_name);
            } else {
                // Analyze all tables
                let tables = engine.list_tables();
                println!("Analyzing {} tables...", tables.len());

                for table_name in &tables {
                    println!("\nAnalyzing table '{}'...", table_name);
                    match engine.collect_table_statistics(table_name) {
                        Ok(stats) => {
                            println!("  Rows: {}", stats.row_count);
                            println!("  Columns: {}", stats.column_stats.len());
                            println!("  Indexes: {}", stats.index_stats.len());
                            optimizer.update_statistics(table_name, stats);
                        }
                        Err(e) => {
                            eprintln!("  Error: {}", e);
                        }
                    }
                }

                println!("\n✓ Statistics updated for all tables");
            }
        }
        Commands::Backup { command } => {
            backup::run(command)?;
        }
        Commands::Optimize {
            data,
            enable,
            stats,
        } => {
            let mut engine = Engine::open(&data).context("Failed to open database")?;

            if stats {
                if let Some(optimizer) = engine.get_query_optimizer() {
                    let stats = optimizer.get_statistics()?;
                    println!("Query Optimization Statistics:");
                    println!("  Queries optimized: {}", stats.queries_optimized);
                    println!("  Cache hits: {}", stats.cache_hits);
                    println!("  Cache misses: {}", stats.cache_misses);
                    println!(
                        "  Avg optimization time: {:.2}ms",
                        stats.avg_optimization_time_ms
                    );
                    println!("  Avg execution time: {:.2}ms", stats.avg_execution_time_ms);
                    println!("  Joins reordered: {}", stats.joins_reordered);
                    println!("  Subqueries flattened: {}", stats.subqueries_flattened);
                    println!("  Indexes suggested: {}", stats.indexes_suggested);
                    println!(
                        "  Materialized views used: {}",
                        stats.materialized_views_used
                    );
                    println!("  Parallel executions: {}", stats.parallel_executions);
                } else {
                    println!("Query optimization is not enabled.");
                }
            } else if enable {
                use driftdb_core::query_performance::OptimizationConfig;
                let config = OptimizationConfig::default();
                engine.enable_query_optimization(config)?;
                println!("Query optimization enabled.");
            } else {
                engine.disable_query_optimization()?;
                println!("Query optimization disabled.");
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
                value,
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
            let seq = s[5..].parse::<u64>().context("Invalid sequence number")?;
            Ok(Some(driftdb_core::query::AsOf::Sequence(seq)))
        }
        Some(s) => {
            let timestamp =
                OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
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
