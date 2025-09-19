pub mod errors;
pub mod events;
pub mod schema;
pub mod storage;
pub mod index;
pub mod snapshot;
pub mod query;
pub mod engine;
pub mod wal;
pub mod observability;
pub mod backup;
pub mod transaction;
pub mod connection;
pub mod migration;
pub mod optimizer;
pub mod encryption;
pub mod replication;

#[cfg(test)]
mod tests;

pub use engine::Engine;
pub use errors::{DriftError, Result};
pub use events::{Event, EventType};
pub use query::{Query, QueryResult};
pub use schema::Schema;
