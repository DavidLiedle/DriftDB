use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventType {
    Insert,
    Patch,
    SoftDelete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub sequence: u64,
    pub timestamp: OffsetDateTime,
    pub event_type: EventType,
    pub table_name: String,
    pub primary_key: Value,
    pub payload: Value,
}

impl Event {
    pub fn new_insert(table_name: String, primary_key: Value, payload: Value) -> Self {
        Self {
            sequence: 0,
            timestamp: OffsetDateTime::now_utc(),
            event_type: EventType::Insert,
            table_name,
            primary_key,
            payload,
        }
    }

    pub fn new_patch(table_name: String, primary_key: Value, payload: Value) -> Self {
        Self {
            sequence: 0,
            timestamp: OffsetDateTime::now_utc(),
            event_type: EventType::Patch,
            table_name,
            primary_key,
            payload,
        }
    }

    pub fn new_soft_delete(table_name: String, primary_key: Value) -> Self {
        Self {
            sequence: 0,
            timestamp: OffsetDateTime::now_utc(),
            event_type: EventType::SoftDelete,
            table_name,
            primary_key,
            payload: Value::Null,
        }
    }
}
