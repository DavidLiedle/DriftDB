pub mod backup;
pub mod connection;
pub mod encryption;
pub mod engine;
pub mod errors;
pub mod events;
pub mod index;
pub mod migration;
pub mod observability;
pub mod optimizer;
pub mod query;
pub mod replication;
pub mod schema;
pub mod snapshot;
pub mod storage;
pub mod transaction;
pub mod wal;

#[cfg(test)]
mod tests;

pub use engine::Engine;
pub use errors::{DriftError, Result};
pub use events::{Event, EventType};
pub use query::{Query, QueryResult};
pub use schema::Schema;
