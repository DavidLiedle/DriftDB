//! Database Triggers Implementation
//!
//! Provides support for database triggers - procedural code that automatically
//! executes in response to certain events on a table.
//!
//! Features:
//! - BEFORE/AFTER triggers
//! - INSERT/UPDATE/DELETE triggers
//! - Row-level and statement-level triggers
//! - Trigger conditions (WHEN clause)
//! - Trigger cascading and recursion control
//! - Temporal triggers for audit trails

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, error, info, trace};

use crate::engine::Engine;
use crate::errors::{DriftError, Result};
use crate::query::QueryResult;
use crate::sql_bridge;

/// Trigger timing - when the trigger fires relative to the event
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TriggerTiming {
    /// Fire before the event
    Before,
    /// Fire after the event
    After,
    /// Fire instead of the event (replaces the event)
    InsteadOf,
}

/// Trigger event that causes the trigger to fire
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Trigger level - granularity of trigger execution
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TriggerLevel {
    /// Fire once per row affected
    Row,
    /// Fire once per statement
    Statement,
}

/// Trigger action - what the trigger does
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerAction {
    /// Execute a SQL statement
    SqlStatement(String),
    /// Call a stored procedure
    CallProcedure { name: String, args: Vec<Value> },
    /// Execute custom code (function name)
    CustomFunction(String),
    /// Log to audit table
    AuditLog {
        table: String,
        include_old: bool,
        include_new: bool,
    },
    /// Validate data with custom logic
    Validate {
        condition: String,
        error_message: String,
    },
    /// Send notification
    Notify { channel: String, payload: Value },
}

/// Trigger definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDefinition {
    /// Unique trigger name
    pub name: String,
    /// Table this trigger is attached to
    pub table_name: String,
    /// When the trigger fires
    pub timing: TriggerTiming,
    /// What events cause the trigger to fire
    pub events: Vec<TriggerEvent>,
    /// Trigger execution level
    pub level: TriggerLevel,
    /// Optional WHEN condition
    pub when_condition: Option<String>,
    /// What the trigger does
    pub action: TriggerAction,
    /// Whether the trigger is enabled
    pub enabled: bool,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Modified timestamp
    pub modified_at: SystemTime,
    /// Trigger owner
    pub owner: String,
    /// Trigger description
    pub description: Option<String>,
}

/// Trigger execution context
#[derive(Debug, Clone)]
pub struct TriggerContext {
    /// The table name
    pub table: String,
    /// The event that triggered this
    pub event: TriggerEvent,
    /// OLD row values (for UPDATE/DELETE)
    pub old_row: Option<Value>,
    /// NEW row values (for INSERT/UPDATE)
    pub new_row: Option<Value>,
    /// Current transaction ID
    pub transaction_id: Option<u64>,
    /// Current user
    pub user: String,
    /// Execution timestamp
    pub timestamp: SystemTime,
    /// Additional metadata
    pub metadata: HashMap<String, Value>,
}

/// Result of trigger execution
#[derive(Debug, Clone)]
pub enum TriggerResult {
    /// Continue with the operation
    Continue,
    /// Skip this row (for row-level triggers)
    Skip,
    /// Abort the operation with an error
    Abort(String),
    /// Replace the new row with modified data
    ModifyRow(Value),
}

/// Trigger manager for handling all triggers in the database
pub struct TriggerManager {
    /// All trigger definitions by table
    triggers_by_table: Arc<RwLock<HashMap<String, Vec<TriggerDefinition>>>>,
    /// All triggers by name
    triggers_by_name: Arc<RwLock<HashMap<String, TriggerDefinition>>>,
    /// Trigger execution statistics
    stats: Arc<RwLock<TriggerStatistics>>,
    /// Maximum recursion depth for cascading triggers
    max_recursion_depth: usize,
    /// Current recursion depth tracking
    recursion_depth: Arc<RwLock<HashMap<u64, usize>>>, // transaction_id -> depth
    /// Database engine for executing trigger SQL
    engine: Option<Arc<RwLock<Engine>>>,
}

/// Trigger execution statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TriggerStatistics {
    pub total_triggers: usize,
    pub enabled_triggers: usize,
    pub total_executions: u64,
    pub successful_executions: u64,
    pub failed_executions: u64,
    pub skipped_rows: u64,
    pub aborted_operations: u64,
    pub avg_execution_time_ms: f64,
}

impl TriggerManager {
    /// Create a new trigger manager
    pub fn new() -> Self {
        Self {
            triggers_by_table: Arc::new(RwLock::new(HashMap::new())),
            triggers_by_name: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(TriggerStatistics::default())),
            max_recursion_depth: 16,
            recursion_depth: Arc::new(RwLock::new(HashMap::new())),
            engine: None,
        }
    }

    /// Set the database engine for executing trigger SQL
    pub fn with_engine(mut self, engine: Arc<RwLock<Engine>>) -> Self {
        self.engine = Some(engine);
        self
    }

    /// Create a new trigger
    pub fn create_trigger(&self, definition: TriggerDefinition) -> Result<()> {
        let trigger_name = definition.name.clone();
        let table_name = definition.table_name.clone();

        debug!(
            "Creating trigger '{}' on table '{}'",
            trigger_name, table_name
        );

        // Check if trigger already exists
        {
            let triggers = self.triggers_by_name.read();
            if triggers.contains_key(&trigger_name) {
                return Err(DriftError::InvalidQuery(format!(
                    "Trigger '{}' already exists",
                    trigger_name
                )));
            }
        }

        // Add trigger to both maps
        {
            let mut by_name = self.triggers_by_name.write();
            by_name.insert(trigger_name.clone(), definition.clone());
        }

        {
            let mut by_table = self.triggers_by_table.write();
            by_table
                .entry(table_name.clone())
                .or_insert_with(Vec::new)
                .push(definition.clone());
        }

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_triggers += 1;
            if definition.enabled {
                stats.enabled_triggers += 1;
            }
        }

        info!(
            "Trigger '{}' created on table '{}'",
            trigger_name, table_name
        );
        Ok(())
    }

    /// Drop a trigger
    pub fn drop_trigger(&self, trigger_name: &str) -> Result<()> {
        debug!("Dropping trigger '{}'", trigger_name);

        // Remove from name map and get definition
        let definition = {
            let mut by_name = self.triggers_by_name.write();
            by_name.remove(trigger_name).ok_or_else(|| {
                DriftError::InvalidQuery(format!("Trigger '{}' does not exist", trigger_name))
            })?
        };

        // Remove from table map
        {
            let mut by_table = self.triggers_by_table.write();
            if let Some(triggers) = by_table.get_mut(&definition.table_name) {
                triggers.retain(|t| t.name != trigger_name);
                if triggers.is_empty() {
                    by_table.remove(&definition.table_name);
                }
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_triggers = stats.total_triggers.saturating_sub(1);
            if definition.enabled {
                stats.enabled_triggers = stats.enabled_triggers.saturating_sub(1);
            }
        }

        info!("Trigger '{}' dropped", trigger_name);
        Ok(())
    }

    /// Enable or disable a trigger
    pub fn set_trigger_enabled(&self, trigger_name: &str, enabled: bool) -> Result<()> {
        let mut by_name = self.triggers_by_name.write();
        let trigger = by_name.get_mut(trigger_name).ok_or_else(|| {
            DriftError::InvalidQuery(format!("Trigger '{}' does not exist", trigger_name))
        })?;

        let was_enabled = trigger.enabled;
        trigger.enabled = enabled;
        trigger.modified_at = SystemTime::now();

        // Update in table map as well
        let table_name = trigger.table_name.clone();
        drop(by_name);

        {
            let mut by_table = self.triggers_by_table.write();
            if let Some(triggers) = by_table.get_mut(&table_name) {
                for t in triggers.iter_mut() {
                    if t.name == trigger_name {
                        t.enabled = enabled;
                        t.modified_at = SystemTime::now();
                        break;
                    }
                }
            }
        }

        // Update statistics
        if was_enabled != enabled {
            let mut stats = self.stats.write();
            if enabled {
                stats.enabled_triggers += 1;
            } else {
                stats.enabled_triggers = stats.enabled_triggers.saturating_sub(1);
            }
        }

        info!(
            "Trigger '{}' {}",
            trigger_name,
            if enabled { "enabled" } else { "disabled" }
        );
        Ok(())
    }

    /// Execute triggers for an event
    pub fn execute_triggers(
        &self,
        context: &TriggerContext,
        timing: TriggerTiming,
    ) -> Result<TriggerResult> {
        let triggers = {
            let by_table = self.triggers_by_table.read();
            by_table.get(&context.table).cloned().unwrap_or_default()
        };

        // Filter triggers that should fire
        let applicable_triggers: Vec<_> = triggers
            .iter()
            .filter(|t| {
                t.enabled
                    && t.timing == timing
                    && t.events.contains(&context.event)
                    && self.evaluate_when_condition(t, context)
            })
            .collect();

        if applicable_triggers.is_empty() {
            return Ok(TriggerResult::Continue);
        }

        debug!(
            "Executing {} triggers for {:?} {:?} on table '{}'",
            applicable_triggers.len(),
            timing,
            context.event,
            context.table
        );

        // Check recursion depth
        if let Some(txn_id) = context.transaction_id {
            let mut depths = self.recursion_depth.write();
            let depth = depths.entry(txn_id).or_insert(0);
            if *depth >= self.max_recursion_depth {
                return Err(DriftError::Internal(format!(
                    "Trigger recursion depth exceeded (max: {})",
                    self.max_recursion_depth
                )));
            }
            *depth += 1;
        }

        let mut result = TriggerResult::Continue;

        for trigger in applicable_triggers {
            let start = std::time::Instant::now();

            match self.execute_single_trigger(trigger, context) {
                Ok(TriggerResult::Continue) => {}
                Ok(TriggerResult::Skip) => {
                    if trigger.level == TriggerLevel::Row {
                        result = TriggerResult::Skip;
                        break;
                    }
                }
                Ok(TriggerResult::Abort(msg)) => {
                    self.update_stats(false, start.elapsed().as_millis() as f64);
                    return Ok(TriggerResult::Abort(msg));
                }
                Ok(TriggerResult::ModifyRow(new_row)) => {
                    if timing == TriggerTiming::Before {
                        result = TriggerResult::ModifyRow(new_row);
                    }
                }
                Err(e) => {
                    error!("Trigger '{}' failed: {}", trigger.name, e);
                    self.update_stats(false, start.elapsed().as_millis() as f64);
                    return Err(e);
                }
            }

            self.update_stats(true, start.elapsed().as_millis() as f64);
        }

        // Clean up recursion tracking
        if let Some(txn_id) = context.transaction_id {
            let mut depths = self.recursion_depth.write();
            if let Some(depth) = depths.get_mut(&txn_id) {
                *depth = depth.saturating_sub(1);
                if *depth == 0 {
                    depths.remove(&txn_id);
                }
            }
        }

        Ok(result)
    }

    /// Execute a single trigger
    fn execute_single_trigger(
        &self,
        trigger: &TriggerDefinition,
        context: &TriggerContext,
    ) -> Result<TriggerResult> {
        trace!("Executing trigger '{}'", trigger.name);

        match &trigger.action {
            TriggerAction::SqlStatement(sql) => {
                // Execute SQL statement
                if let Some(ref engine_arc) = self.engine {
                    // Replace placeholders in SQL with trigger context values
                    let mut bound_sql = sql.clone();

                    // Replace OLD and NEW row references
                    if let Some(ref old_row) = context.old_row {
                        for (key, value) in old_row.as_object().unwrap_or(&serde_json::Map::new()) {
                            let placeholder = format!("OLD.{}", key);
                            let value_str = match value {
                                Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                Value::Number(n) => n.to_string(),
                                Value::Bool(b) => b.to_string(),
                                Value::Null => "NULL".to_string(),
                                _ => format!("'{}'", value.to_string().replace('\'', "''")),
                            };
                            bound_sql = bound_sql.replace(&placeholder, &value_str);
                        }
                    }

                    if let Some(ref new_row) = context.new_row {
                        for (key, value) in new_row.as_object().unwrap_or(&serde_json::Map::new()) {
                            let placeholder = format!("NEW.{}", key);
                            let value_str = match value {
                                Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                Value::Number(n) => n.to_string(),
                                Value::Bool(b) => b.to_string(),
                                Value::Null => "NULL".to_string(),
                                _ => format!("'{}'", value.to_string().replace('\'', "''")),
                            };
                            bound_sql = bound_sql.replace(&placeholder, &value_str);
                        }
                    }

                    debug!("Executing trigger SQL: {}", bound_sql);

                    // Execute the SQL
                    let mut engine = engine_arc.write();
                    match sql_bridge::execute_sql(&mut engine, &bound_sql) {
                        Ok(QueryResult::Success { .. }) => {
                            debug!("Trigger SQL executed successfully");
                            Ok(TriggerResult::Continue)
                        }
                        Ok(QueryResult::Rows { .. }) => {
                            debug!("Trigger SQL executed and returned rows");
                            Ok(TriggerResult::Continue)
                        }
                        Ok(QueryResult::DriftHistory { .. }) => {
                            debug!("Trigger SQL executed and returned history");
                            Ok(TriggerResult::Continue)
                        }
                        Ok(QueryResult::Plan { .. }) => {
                            debug!("Trigger SQL returned query plan (EXPLAIN)");
                            Ok(TriggerResult::Continue)
                        }
                        Ok(QueryResult::Error { message }) => {
                            error!("Trigger SQL execution failed: {}", message);
                            Ok(TriggerResult::Abort(format!("Trigger failed: {}", message)))
                        }
                        Err(e) => {
                            error!("Trigger SQL execution error: {}", e);
                            Ok(TriggerResult::Abort(format!("Trigger error: {}", e)))
                        }
                    }
                } else {
                    debug!("No engine available for trigger SQL execution");
                    Ok(TriggerResult::Continue)
                }
            }
            TriggerAction::CallProcedure { name, args } => {
                // Call stored procedure
                if let Some(ref engine_arc) = self.engine {
                    // Build CALL statement
                    let arg_strings: Vec<String> = args
                        .iter()
                        .map(|arg| match arg {
                            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => "NULL".to_string(),
                            _ => format!("'{}'", arg.to_string().replace('\'', "''")),
                        })
                        .collect();

                    let call_sql = format!("CALL {}({})", name, arg_strings.join(", "));
                    debug!("Executing procedure call: {}", call_sql);

                    let mut engine = engine_arc.write();
                    match sql_bridge::execute_sql(&mut engine, &call_sql) {
                        Ok(_) => {
                            debug!("Procedure call executed successfully");
                            Ok(TriggerResult::Continue)
                        }
                        Err(e) => {
                            error!("Procedure call execution error: {}", e);
                            Ok(TriggerResult::Abort(format!("Procedure call error: {}", e)))
                        }
                    }
                } else {
                    debug!("No engine available for procedure call execution");
                    Ok(TriggerResult::Continue)
                }
            }
            TriggerAction::CustomFunction(func_name) => {
                // TODO: Execute custom function
                debug!("Would execute function '{}'", func_name);
                Ok(TriggerResult::Continue)
            }
            TriggerAction::AuditLog {
                table,
                include_old,
                include_new,
            } => self.execute_audit_log(table, *include_old, *include_new, context),
            TriggerAction::Validate {
                condition,
                error_message,
            } => {
                if !self.evaluate_condition(condition, context) {
                    Ok(TriggerResult::Abort(error_message.clone()))
                } else {
                    Ok(TriggerResult::Continue)
                }
            }
            TriggerAction::Notify {
                channel,
                payload: _payload,
            } => {
                // TODO: Send notification
                debug!("Would notify channel '{}' with payload", channel);
                Ok(TriggerResult::Continue)
            }
        }
    }

    /// Execute audit log trigger action
    fn execute_audit_log(
        &self,
        audit_table: &str,
        include_old: bool,
        include_new: bool,
        context: &TriggerContext,
    ) -> Result<TriggerResult> {
        let mut audit_entry = json!({
            "table_name": context.table,
            "operation": format!("{:?}", context.event),
            "timestamp": context.timestamp.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default().as_secs(),
            "user": context.user,
        });

        if let Some(txn_id) = context.transaction_id {
            audit_entry["transaction_id"] = json!(txn_id);
        }

        if include_old {
            if let Some(old) = &context.old_row {
                audit_entry["old_values"] = old.clone();
            }
        }

        if include_new {
            if let Some(new) = &context.new_row {
                audit_entry["new_values"] = new.clone();
            }
        }

        // Actually insert into audit table
        if let Some(ref engine_arc) = self.engine {
            // Create INSERT statement for audit table
            let columns: Vec<String> = audit_entry.as_object().unwrap().keys().cloned().collect();
            let values: Vec<String> = audit_entry
                .as_object()
                .unwrap()
                .values()
                .map(|v| match v {
                    Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "NULL".to_string(),
                    _ => format!("'{}'", v.to_string().replace('\'', "''")),
                })
                .collect();

            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                audit_table,
                columns.join(", "),
                values.join(", ")
            );

            debug!("Inserting audit entry: {}", insert_sql);

            let mut engine = engine_arc.write();
            match sql_bridge::execute_sql(&mut engine, &insert_sql) {
                Ok(_) => {
                    debug!("Audit entry inserted successfully");
                    Ok(TriggerResult::Continue)
                }
                Err(e) => {
                    error!("Failed to insert audit entry: {}", e);
                    // Don't fail the trigger, just log the error
                    Ok(TriggerResult::Continue)
                }
            }
        } else {
            debug!("No engine available for audit log insertion");
            Ok(TriggerResult::Continue)
        }
    }

    /// Evaluate WHEN condition for a trigger
    fn evaluate_when_condition(
        &self,
        trigger: &TriggerDefinition,
        context: &TriggerContext,
    ) -> bool {
        if let Some(condition) = &trigger.when_condition {
            self.evaluate_condition(condition, context)
        } else {
            true
        }
    }

    /// Evaluate a condition expression
    fn evaluate_condition(&self, condition: &str, context: &TriggerContext) -> bool {
        // Simple condition evaluation - in a real implementation, this would parse SQL expressions

        // Handle some common patterns
        if condition.is_empty() {
            return true;
        }

        // Replace OLD and NEW references with actual values
        let mut eval_condition = condition.to_string();

        if let Some(ref old_row) = context.old_row {
            for (key, value) in old_row.as_object().unwrap_or(&serde_json::Map::new()) {
                let placeholder = format!("OLD.{}", key);
                let value_str = match value {
                    Value::String(s) => format!("'{}'", s),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "NULL".to_string(),
                    _ => format!("'{}'", value.to_string()),
                };
                eval_condition = eval_condition.replace(&placeholder, &value_str);
            }
        }

        if let Some(ref new_row) = context.new_row {
            for (key, value) in new_row.as_object().unwrap_or(&serde_json::Map::new()) {
                let placeholder = format!("NEW.{}", key);
                let value_str = match value {
                    Value::String(s) => format!("'{}'", s),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "NULL".to_string(),
                    _ => format!("'{}'", value.to_string()),
                };
                eval_condition = eval_condition.replace(&placeholder, &value_str);
            }
        }

        // For simple conditions, we can execute them as SQL queries
        if let Some(ref engine_arc) = self.engine {
            let check_sql = format!(
                "SELECT CASE WHEN ({}) THEN 1 ELSE 0 END AS result",
                eval_condition
            );

            let mut engine = engine_arc.write();
            match sql_bridge::execute_sql(&mut engine, &check_sql) {
                Ok(QueryResult::Rows { data }) => {
                    if let Some(first_row) = data.first() {
                        if let Some(result) = first_row.get("result") {
                            return result.as_i64().unwrap_or(0) == 1;
                        }
                    }
                    false
                }
                _ => {
                    debug!("Failed to evaluate condition: {}", condition);
                    true // Default to true if evaluation fails
                }
            }
        } else {
            // Without engine access, do basic string matching for common patterns
            if eval_condition.contains(" = ") || eval_condition.contains(" == ") {
                // Very basic equality check
                true // Simplified for now
            } else {
                true
            }
        }
    }

    /// Update execution statistics
    fn update_stats(&self, success: bool, execution_time_ms: f64) {
        let mut stats = self.stats.write();
        stats.total_executions += 1;

        if success {
            stats.successful_executions += 1;
        } else {
            stats.failed_executions += 1;
        }

        // Update average execution time
        let total_time = stats.avg_execution_time_ms * (stats.total_executions - 1) as f64;
        stats.avg_execution_time_ms =
            (total_time + execution_time_ms) / stats.total_executions as f64;
    }

    /// Get trigger by name
    pub fn get_trigger(&self, trigger_name: &str) -> Option<TriggerDefinition> {
        self.triggers_by_name.read().get(trigger_name).cloned()
    }

    /// List all triggers
    pub fn list_triggers(&self) -> Vec<TriggerDefinition> {
        self.triggers_by_name.read().values().cloned().collect()
    }

    /// List triggers for a table
    pub fn list_table_triggers(&self, table_name: &str) -> Vec<TriggerDefinition> {
        self.triggers_by_table
            .read()
            .get(table_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Get trigger statistics
    pub fn statistics(&self) -> TriggerStatistics {
        self.stats.read().clone()
    }
}

/// Builder for creating trigger definitions
pub struct TriggerBuilder {
    name: String,
    table_name: String,
    timing: TriggerTiming,
    events: Vec<TriggerEvent>,
    level: TriggerLevel,
    when_condition: Option<String>,
    action: TriggerAction,
    owner: String,
    description: Option<String>,
}

impl TriggerBuilder {
    /// Create a new trigger builder
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table_name: table.into(),
            timing: TriggerTiming::After,
            events: vec![],
            level: TriggerLevel::Row,
            when_condition: None,
            action: TriggerAction::SqlStatement(String::new()),
            owner: "system".to_string(),
            description: None,
        }
    }

    /// Set trigger timing
    pub fn timing(mut self, timing: TriggerTiming) -> Self {
        self.timing = timing;
        self
    }

    /// Add trigger event
    pub fn on_event(mut self, event: TriggerEvent) -> Self {
        self.events.push(event);
        self
    }

    /// Set trigger level
    pub fn level(mut self, level: TriggerLevel) -> Self {
        self.level = level;
        self
    }

    /// Set WHEN condition
    pub fn when_condition(mut self, condition: impl Into<String>) -> Self {
        self.when_condition = Some(condition.into());
        self
    }

    /// Set trigger action
    pub fn action(mut self, action: TriggerAction) -> Self {
        self.action = action;
        self
    }

    /// Set owner
    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
    }

    /// Set description
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Build the trigger definition
    pub fn build(self) -> Result<TriggerDefinition> {
        if self.events.is_empty() {
            return Err(DriftError::InvalidQuery(
                "Trigger must have at least one event".to_string(),
            ));
        }

        Ok(TriggerDefinition {
            name: self.name,
            table_name: self.table_name,
            timing: self.timing,
            events: self.events,
            level: self.level,
            when_condition: self.when_condition,
            action: self.action,
            enabled: true,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            owner: self.owner,
            description: self.description,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_creation() {
        let manager = TriggerManager::new();

        let trigger = TriggerBuilder::new("audit_users", "users")
            .timing(TriggerTiming::After)
            .on_event(TriggerEvent::Insert)
            .on_event(TriggerEvent::Update)
            .action(TriggerAction::AuditLog {
                table: "audit_log".to_string(),
                include_old: true,
                include_new: true,
            })
            .description("Audit all changes to users table")
            .build()
            .unwrap();

        manager.create_trigger(trigger).unwrap();

        let retrieved = manager.get_trigger("audit_users").unwrap();
        assert_eq!(retrieved.name, "audit_users");
        assert_eq!(retrieved.table_name, "users");
    }

    #[test]
    fn test_trigger_enable_disable() {
        let manager = TriggerManager::new();

        let trigger = TriggerBuilder::new("test_trigger", "test_table")
            .on_event(TriggerEvent::Insert)
            .build()
            .unwrap();

        manager.create_trigger(trigger).unwrap();

        // Disable trigger
        manager.set_trigger_enabled("test_trigger", false).unwrap();
        let trigger = manager.get_trigger("test_trigger").unwrap();
        assert!(!trigger.enabled);

        // Re-enable trigger
        manager.set_trigger_enabled("test_trigger", true).unwrap();
        let trigger = manager.get_trigger("test_trigger").unwrap();
        assert!(trigger.enabled);
    }

    #[test]
    fn test_validation_trigger() {
        let trigger = TriggerBuilder::new("validate_age", "users")
            .timing(TriggerTiming::Before)
            .on_event(TriggerEvent::Insert)
            .on_event(TriggerEvent::Update)
            .when_condition("NEW.age IS NOT NULL")
            .action(TriggerAction::Validate {
                condition: "NEW.age >= 18 AND NEW.age <= 120".to_string(),
                error_message: "Age must be between 18 and 120".to_string(),
            })
            .build()
            .unwrap();

        assert_eq!(trigger.timing, TriggerTiming::Before);
        assert!(trigger.when_condition.is_some());
    }
}
