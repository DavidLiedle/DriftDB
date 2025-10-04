use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub root: Arc<PlanNode>,
    pub estimated_cost: f64,
    pub estimated_rows: usize,
    pub optimization_rules_applied: Vec<String>,
    pub statistics_used: bool,
    pub parallelism_enabled: bool,
    pub cache_hits_expected: usize,
}

#[derive(Debug, Clone)]
pub struct PlanNode {
    pub id: usize,
    pub operation: PlanOperation,
    pub children: Vec<Arc<PlanNode>>,
    pub estimated_cost: f64,
    pub estimated_rows: usize,
    pub actual_rows: Option<usize>,
    pub actual_time_ms: Option<f64>,
    pub properties: NodeProperties,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanOperation {
    TableScan {
        table: String,
        columns: Vec<String>,
        filter: Option<String>,
    },
    IndexScan {
        table: String,
        index: String,
        columns: Vec<String>,
        range: Option<ScanRange>,
    },
    NestedLoopJoin {
        join_type: JoinType,
        condition: String,
    },
    HashJoin {
        join_type: JoinType,
        condition: String,
        build_side: BuildSide,
    },
    MergeJoin {
        join_type: JoinType,
        condition: String,
        sort_keys: Vec<SortKey>,
    },
    Sort {
        keys: Vec<SortKey>,
        limit: Option<usize>,
    },
    Aggregate {
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
    },
    Filter {
        predicate: String,
    },
    Project {
        expressions: Vec<String>,
    },
    Union {
        all: bool,
    },
    Intersect,
    Except,
    Limit {
        count: usize,
        offset: usize,
    },
    MaterializedView {
        view: String,
    },
    CacheLookup {
        cache_key: String,
    },
    ParallelScan {
        partitions: usize,
    },
    WindowFunction {
        function: String,
        partition_by: Vec<String>,
        order_by: Vec<SortKey>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeProperties {
    pub selectivity: f64,
    pub cpu_cost: f64,
    pub io_cost: f64,
    pub memory_usage: usize,
    pub network_cost: f64,
    pub parallelism: Option<usize>,
    pub pipeline_breaker: bool,
    pub can_use_index: bool,
    pub distributed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    Semi,
    AntiSemi,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BuildSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanRange {
    pub start: Option<String>,
    pub end: Option<String>,
    pub start_inclusive: bool,
    pub end_inclusive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateFunction {
    pub function: String,
    pub column: String,
    pub alias: String,
}

pub struct QueryPlanBuilder {
    next_id: usize,
    optimization_rules: Vec<Box<dyn OptimizationRule>>,
}

impl QueryPlanBuilder {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            optimization_rules: vec![
                Box::new(PushDownPredicates),
                Box::new(EliminateSubqueries),
                Box::new(JoinReordering),
                Box::new(IndexSelection),
                Box::new(PartitionPruning),
                Box::new(CommonSubexpressionElimination),
            ],
        }
    }

    pub fn build_plan(&mut self, query: &str) -> Result<QueryPlan> {
        let initial_plan = self.parse_query(query)?;
        let optimized_plan = self.optimize_plan(initial_plan)?;
        Ok(optimized_plan)
    }

    fn parse_query(&mut self, _query: &str) -> Result<QueryPlan> {
        let root = self.create_node(
            PlanOperation::TableScan {
                table: "default".to_string(),
                columns: vec!["*".to_string()],
                filter: None,
            },
            vec![],
        );

        Ok(QueryPlan {
            root,
            estimated_cost: 100.0,
            estimated_rows: 1000,
            optimization_rules_applied: vec![],
            statistics_used: false,
            parallelism_enabled: false,
            cache_hits_expected: 0,
        })
    }

    fn optimize_plan(&self, mut plan: QueryPlan) -> Result<QueryPlan> {
        for rule in &self.optimization_rules {
            if rule.applicable(&plan) {
                plan = rule.apply(plan)?;
                plan.optimization_rules_applied.push(rule.name());
            }
        }
        Ok(plan)
    }

    fn create_node(
        &mut self,
        operation: PlanOperation,
        children: Vec<Arc<PlanNode>>,
    ) -> Arc<PlanNode> {
        let id = self.next_id;
        self.next_id += 1;

        let properties = NodeProperties {
            selectivity: 1.0,
            cpu_cost: 1.0,
            io_cost: 1.0,
            memory_usage: 1024,
            network_cost: 0.0,
            parallelism: None,
            pipeline_breaker: false,
            can_use_index: false,
            distributed: false,
        };

        Arc::new(PlanNode {
            id,
            operation,
            children,
            estimated_cost: 1.0,
            estimated_rows: 100,
            actual_rows: None,
            actual_time_ms: None,
            properties,
        })
    }
}

trait OptimizationRule: Send + Sync {
    fn name(&self) -> String;
    fn applicable(&self, plan: &QueryPlan) -> bool;
    fn apply(&self, plan: QueryPlan) -> Result<QueryPlan>;
}

struct PushDownPredicates;
impl OptimizationRule for PushDownPredicates {
    fn name(&self) -> String {
        "PushDownPredicates".to_string()
    }

    fn applicable(&self, _plan: &QueryPlan) -> bool {
        true
    }

    fn apply(&self, plan: QueryPlan) -> Result<QueryPlan> {
        Ok(plan)
    }
}

struct EliminateSubqueries;
impl OptimizationRule for EliminateSubqueries {
    fn name(&self) -> String {
        "EliminateSubqueries".to_string()
    }

    fn applicable(&self, _plan: &QueryPlan) -> bool {
        false
    }

    fn apply(&self, plan: QueryPlan) -> Result<QueryPlan> {
        Ok(plan)
    }
}

struct JoinReordering;
impl OptimizationRule for JoinReordering {
    fn name(&self) -> String {
        "JoinReordering".to_string()
    }

    fn applicable(&self, _plan: &QueryPlan) -> bool {
        false
    }

    fn apply(&self, plan: QueryPlan) -> Result<QueryPlan> {
        Ok(plan)
    }
}

struct IndexSelection;
impl OptimizationRule for IndexSelection {
    fn name(&self) -> String {
        "IndexSelection".to_string()
    }

    fn applicable(&self, _plan: &QueryPlan) -> bool {
        true
    }

    fn apply(&self, plan: QueryPlan) -> Result<QueryPlan> {
        Ok(plan)
    }
}

struct PartitionPruning;
impl OptimizationRule for PartitionPruning {
    fn name(&self) -> String {
        "PartitionPruning".to_string()
    }

    fn applicable(&self, _plan: &QueryPlan) -> bool {
        false
    }

    fn apply(&self, plan: QueryPlan) -> Result<QueryPlan> {
        Ok(plan)
    }
}

struct CommonSubexpressionElimination;
impl OptimizationRule for CommonSubexpressionElimination {
    fn name(&self) -> String {
        "CommonSubexpressionElimination".to_string()
    }

    fn applicable(&self, _plan: &QueryPlan) -> bool {
        false
    }

    fn apply(&self, plan: QueryPlan) -> Result<QueryPlan> {
        Ok(plan)
    }
}

#[derive(Debug, Clone)]
pub struct QueryPlanVisualizer {
    format: VisualizationFormat,
    show_costs: bool,
    show_actual_stats: bool,
    show_properties: bool,
}

#[derive(Debug, Clone)]
pub enum VisualizationFormat {
    Text,
    Json,
    Dot,
    Html,
}

impl QueryPlanVisualizer {
    pub fn new(format: VisualizationFormat) -> Self {
        Self {
            format,
            show_costs: true,
            show_actual_stats: true,
            show_properties: false,
        }
    }

    pub fn visualize(&self, plan: &QueryPlan) -> String {
        match self.format {
            VisualizationFormat::Text => self.visualize_text(plan),
            VisualizationFormat::Json => self.visualize_json(plan),
            VisualizationFormat::Dot => self.visualize_dot(plan),
            VisualizationFormat::Html => self.visualize_html(plan),
        }
    }

    fn visualize_text(&self, plan: &QueryPlan) -> String {
        let mut output = String::new();
        output.push_str("Query Execution Plan\n");
        output.push_str("====================\n\n");

        if !plan.optimization_rules_applied.is_empty() {
            output.push_str("Optimizations Applied:\n");
            for rule in &plan.optimization_rules_applied {
                output.push_str(&format!("  - {}\n", rule));
            }
            output.push_str("\n");
        }

        output.push_str(&format!("Estimated Cost: {:.2}\n", plan.estimated_cost));
        output.push_str(&format!("Estimated Rows: {}\n\n", plan.estimated_rows));

        self.visualize_node_text(&plan.root, &mut output, 0);
        output
    }

    fn visualize_node_text(&self, node: &PlanNode, output: &mut String, depth: usize) {
        let indent = "  ".repeat(depth);

        output.push_str(&format!(
            "{}-> {}\n",
            indent,
            self.format_operation(&node.operation)
        ));

        if self.show_costs {
            output.push_str(&format!(
                "{}   Cost: {:.2}, Rows: {}",
                indent, node.estimated_cost, node.estimated_rows
            ));

            if let Some(actual_rows) = node.actual_rows {
                output.push_str(&format!(" (actual: {})", actual_rows));
            }
            output.push_str("\n");
        }

        if self.show_actual_stats && node.actual_time_ms.is_some() {
            output.push_str(&format!(
                "{}   Time: {:.2}ms\n",
                indent,
                node.actual_time_ms.unwrap()
            ));
        }

        if self.show_properties {
            output.push_str(&format!(
                "{}   Properties: CPU={:.2}, IO={:.2}, Mem={}KB\n",
                indent,
                node.properties.cpu_cost,
                node.properties.io_cost,
                node.properties.memory_usage / 1024
            ));
        }

        for child in &node.children {
            self.visualize_node_text(child, output, depth + 1);
        }
    }

    fn visualize_json(&self, plan: &QueryPlan) -> String {
        format!(
            r#"{{
  "estimatedCost": {},
  "estimatedRows": {},
  "optimizationsApplied": {:?},
  "statisticsUsed": {},
  "parallelismEnabled": {},
  "cacheHitsExpected": {}
}}"#,
            plan.estimated_cost,
            plan.estimated_rows,
            plan.optimization_rules_applied,
            plan.statistics_used,
            plan.parallelism_enabled,
            plan.cache_hits_expected
        )
    }

    fn visualize_dot(&self, plan: &QueryPlan) -> String {
        let mut output = String::new();
        output.push_str("digraph QueryPlan {\n");
        output.push_str("  rankdir=BT;\n");
        output.push_str("  node [shape=box];\n\n");

        self.visualize_node_dot(&plan.root, &mut output, &mut HashSet::new());

        output.push_str("}\n");
        output
    }

    fn visualize_node_dot(
        &self,
        node: &PlanNode,
        output: &mut String,
        visited: &mut HashSet<usize>,
    ) {
        if visited.contains(&node.id) {
            return;
        }
        visited.insert(node.id);

        let label = self.format_operation(&node.operation);
        let cost_label = if self.show_costs {
            format!(
                "\\nCost: {:.2}\\nRows: {}",
                node.estimated_cost, node.estimated_rows
            )
        } else {
            String::new()
        };

        output.push_str(&format!(
            "  n{} [label=\"{}{}\"]\n",
            node.id, label, cost_label
        ));

        for child in &node.children {
            output.push_str(&format!("  n{} -> n{}\n", child.id, node.id));
            self.visualize_node_dot(child, output, visited);
        }
    }

    fn visualize_html(&self, plan: &QueryPlan) -> String {
        let mut html = String::new();
        html.push_str("<!DOCTYPE html>\n<html>\n<head>\n");
        html.push_str("<style>\n");
        html.push_str("body { font-family: monospace; }\n");
        html.push_str(".node { margin-left: 20px; padding: 5px; border-left: 2px solid #ccc; }\n");
        html.push_str(".operation { font-weight: bold; color: #2e7d32; }\n");
        html.push_str(".stats { color: #666; font-size: 0.9em; }\n");
        html.push_str("</style>\n");
        html.push_str("</head>\n<body>\n");
        html.push_str("<h1>Query Execution Plan</h1>\n");

        if !plan.optimization_rules_applied.is_empty() {
            html.push_str("<h3>Optimizations Applied:</h3>\n<ul>\n");
            for rule in &plan.optimization_rules_applied {
                html.push_str(&format!("<li>{}</li>\n", rule));
            }
            html.push_str("</ul>\n");
        }

        html.push_str(&format!(
            "<p>Estimated Cost: {:.2}<br>",
            plan.estimated_cost
        ));
        html.push_str(&format!("Estimated Rows: {}</p>\n", plan.estimated_rows));

        html.push_str("<div class=\"plan\">\n");
        self.visualize_node_html(&plan.root, &mut html);
        html.push_str("</div>\n");

        html.push_str("</body>\n</html>\n");
        html
    }

    fn visualize_node_html(&self, node: &PlanNode, output: &mut String) {
        output.push_str("<div class=\"node\">\n");
        output.push_str(&format!(
            "<div class=\"operation\">{}</div>\n",
            self.format_operation(&node.operation)
        ));

        if self.show_costs {
            output.push_str("<div class=\"stats\">");
            output.push_str(&format!(
                "Cost: {:.2}, Rows: {}",
                node.estimated_cost, node.estimated_rows
            ));

            if let Some(actual_rows) = node.actual_rows {
                output.push_str(&format!(" (actual: {})", actual_rows));
            }

            if let Some(time_ms) = node.actual_time_ms {
                output.push_str(&format!(" - Time: {:.2}ms", time_ms));
            }

            output.push_str("</div>\n");
        }

        for child in &node.children {
            self.visualize_node_html(child, output);
        }

        output.push_str("</div>\n");
    }

    fn format_operation(&self, op: &PlanOperation) -> String {
        match op {
            PlanOperation::TableScan {
                table,
                columns,
                filter,
            } => {
                let filter_str = filter
                    .as_ref()
                    .map(|f| format!(" WHERE {}", f))
                    .unwrap_or_default();
                format!(
                    "TableScan({}, [{}]{})",
                    table,
                    columns.join(", "),
                    filter_str
                )
            }
            PlanOperation::IndexScan { table, index, .. } => {
                format!("IndexScan({} using {})", table, index)
            }
            PlanOperation::HashJoin {
                join_type,
                condition,
                ..
            } => {
                format!("HashJoin({:?} ON {})", join_type, condition)
            }
            PlanOperation::NestedLoopJoin {
                join_type,
                condition,
            } => {
                format!("NestedLoopJoin({:?} ON {})", join_type, condition)
            }
            PlanOperation::MergeJoin {
                join_type,
                condition,
                ..
            } => {
                format!("MergeJoin({:?} ON {})", join_type, condition)
            }
            PlanOperation::Sort { keys, limit } => {
                let keys_str = keys
                    .iter()
                    .map(|k| format!("{} {}", k.column, if k.ascending { "ASC" } else { "DESC" }))
                    .collect::<Vec<_>>()
                    .join(", ");
                let limit_str = limit.map(|l| format!(" LIMIT {}", l)).unwrap_or_default();
                format!("Sort([{}]{})", keys_str, limit_str)
            }
            PlanOperation::Aggregate {
                group_by,
                aggregates,
            } => {
                let group_str = if group_by.is_empty() {
                    String::new()
                } else {
                    format!("GROUP BY [{}] ", group_by.join(", "))
                };
                let agg_str = aggregates
                    .iter()
                    .map(|a| format!("{}({})", a.function, a.column))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("Aggregate({}[{}])", group_str, agg_str)
            }
            PlanOperation::Filter { predicate } => {
                format!("Filter({})", predicate)
            }
            PlanOperation::Project { expressions } => {
                format!("Project([{}])", expressions.join(", "))
            }
            PlanOperation::Limit { count, offset } => {
                if *offset > 0 {
                    format!("Limit({} OFFSET {})", count, offset)
                } else {
                    format!("Limit({})", count)
                }
            }
            PlanOperation::Union { all } => {
                format!("Union{}", if *all { " ALL" } else { "" })
            }
            PlanOperation::Intersect => "Intersect".to_string(),
            PlanOperation::Except => "Except".to_string(),
            PlanOperation::MaterializedView { view } => {
                format!("MaterializedView({})", view)
            }
            PlanOperation::CacheLookup { cache_key } => {
                format!("CacheLookup({})", cache_key)
            }
            PlanOperation::ParallelScan { partitions } => {
                format!("ParallelScan({} partitions)", partitions)
            }
            PlanOperation::WindowFunction {
                function,
                partition_by,
                ..
            } => {
                let partition_str = if partition_by.is_empty() {
                    String::new()
                } else {
                    format!(" PARTITION BY [{}]", partition_by.join(", "))
                };
                format!("WindowFunction({}{})", function, partition_str)
            }
        }
    }
}

pub struct PlanAnalyzer {
    warnings: Vec<String>,
    suggestions: Vec<String>,
}

impl PlanAnalyzer {
    pub fn new() -> Self {
        Self {
            warnings: vec![],
            suggestions: vec![],
        }
    }

    pub fn analyze(&mut self, plan: &QueryPlan) -> AnalysisReport {
        self.warnings.clear();
        self.suggestions.clear();

        self.analyze_node(&plan.root);
        self.check_performance_issues(plan);
        self.suggest_optimizations(plan);

        AnalysisReport {
            warnings: self.warnings.clone(),
            suggestions: self.suggestions.clone(),
            estimated_memory_mb: self.estimate_memory_usage(&plan.root) / (1024 * 1024),
            parallelism_opportunities: self.find_parallelism_opportunities(&plan.root),
            index_recommendations: self.recommend_indexes(&plan.root),
            join_order_optimal: self.check_join_order(&plan.root),
        }
    }

    fn analyze_node(&mut self, node: &PlanNode) {
        match &node.operation {
            PlanOperation::TableScan { filter, .. } if filter.is_none() => {
                self.warnings
                    .push("Full table scan detected without filter".to_string());
            }
            PlanOperation::NestedLoopJoin { .. } if node.estimated_rows > 10000 => {
                self.warnings
                    .push("Nested loop join on large dataset".to_string());
                self.suggestions
                    .push("Consider using hash join for better performance".to_string());
            }
            PlanOperation::Sort { .. } if node.properties.memory_usage > 100 * 1024 * 1024 => {
                self.warnings
                    .push("Large sort operation may spill to disk".to_string());
            }
            _ => {}
        }

        for child in &node.children {
            self.analyze_node(child);
        }
    }

    fn check_performance_issues(&mut self, plan: &QueryPlan) {
        if plan.estimated_cost > 10000.0 {
            self.warnings
                .push("Query has high estimated cost".to_string());
        }

        if !plan.statistics_used {
            self.suggestions
                .push("Update table statistics for better optimization".to_string());
        }

        if !plan.parallelism_enabled && plan.estimated_rows > 100000 {
            self.suggestions
                .push("Enable parallel query execution for large datasets".to_string());
        }
    }

    fn suggest_optimizations(&mut self, _plan: &QueryPlan) {
        if self.suggestions.is_empty() {
            self.suggestions
                .push("Query plan appears optimal".to_string());
        }
    }

    fn estimate_memory_usage(&self, node: &PlanNode) -> usize {
        let mut total = node.properties.memory_usage;
        for child in &node.children {
            total += self.estimate_memory_usage(child);
        }
        total
    }

    fn find_parallelism_opportunities(&self, node: &PlanNode) -> Vec<String> {
        let mut opportunities = vec![];

        match &node.operation {
            PlanOperation::TableScan { table, .. } if node.estimated_rows > 50000 => {
                opportunities.push(format!("Parallelize scan of table {}", table));
            }
            PlanOperation::Aggregate { .. } if node.estimated_rows > 10000 => {
                opportunities.push("Use parallel aggregation".to_string());
            }
            _ => {}
        }

        for child in &node.children {
            opportunities.extend(self.find_parallelism_opportunities(child));
        }

        opportunities
    }

    fn recommend_indexes(&self, node: &PlanNode) -> Vec<IndexRecommendation> {
        let mut recommendations = vec![];

        if let PlanOperation::TableScan { table, filter, .. } = &node.operation {
            if let Some(filter_str) = filter {
                recommendations.push(IndexRecommendation {
                    table: table.clone(),
                    columns: vec![filter_str.clone()],
                    index_type: "btree".to_string(),
                    estimated_benefit: node.estimated_cost * 0.8,
                });
            }
        }

        for child in &node.children {
            recommendations.extend(self.recommend_indexes(child));
        }

        recommendations
    }

    fn check_join_order(&self, _node: &PlanNode) -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisReport {
    pub warnings: Vec<String>,
    pub suggestions: Vec<String>,
    pub estimated_memory_mb: usize,
    pub parallelism_opportunities: Vec<String>,
    pub index_recommendations: Vec<IndexRecommendation>,
    pub join_order_optimal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRecommendation {
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: String,
    pub estimated_benefit: f64,
}

pub struct PlanExecutionTracker {
    start_time: Instant,
    node_timings: HashMap<usize, NodeTiming>,
}

#[derive(Debug, Clone)]
struct NodeTiming {
    start: Instant,
    end: Option<Instant>,
    rows_processed: usize,
    memory_peak: usize,
}

impl PlanExecutionTracker {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            node_timings: HashMap::new(),
        }
    }

    pub fn start_node(&mut self, node_id: usize) {
        self.node_timings.insert(
            node_id,
            NodeTiming {
                start: Instant::now(),
                end: None,
                rows_processed: 0,
                memory_peak: 0,
            },
        );
    }

    pub fn end_node(&mut self, node_id: usize, rows: usize) {
        if let Some(timing) = self.node_timings.get_mut(&node_id) {
            timing.end = Some(Instant::now());
            timing.rows_processed = rows;
        }
    }

    pub fn update_memory(&mut self, node_id: usize, memory: usize) {
        if let Some(timing) = self.node_timings.get_mut(&node_id) {
            timing.memory_peak = timing.memory_peak.max(memory);
        }
    }

    pub fn generate_profile(&self, plan: &mut QueryPlan) {
        self.update_node_stats(&mut Arc::get_mut(&mut plan.root).unwrap());
    }

    fn update_node_stats(&self, node: &mut PlanNode) {
        if let Some(timing) = self.node_timings.get(&node.id) {
            node.actual_rows = Some(timing.rows_processed);
            if let Some(end) = timing.end {
                let duration = end.duration_since(timing.start);
                node.actual_time_ms = Some(duration.as_secs_f64() * 1000.0);
            }
        }

        for child in &mut node.children {
            if let Some(child_mut) = Arc::get_mut(child) {
                self.update_node_stats(child_mut);
            }
        }
    }

    pub fn total_execution_time(&self) -> Duration {
        Instant::now().duration_since(self.start_time)
    }
}

impl fmt::Display for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let visualizer = QueryPlanVisualizer::new(VisualizationFormat::Text);
        write!(f, "{}", visualizer.visualize(self))
    }
}
