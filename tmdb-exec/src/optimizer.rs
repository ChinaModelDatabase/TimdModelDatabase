/// Cost-based query optimizer with join rewrite and subquery optimization
use std::collections::HashMap;

/// Query plan node
#[derive(Debug, Clone)]
pub enum PlanNode {
    SeqScan { table: String, estimated_rows: u64, estimated_cost: f64 },
    IndexScan { table: String, index: String, estimated_rows: u64, estimated_cost: f64 },
    Filter { input: Box<PlanNode>, predicate: String, selectivity: f64 },
    HashJoin { left: Box<PlanNode>, right: Box<PlanNode>, join_key: String, join_type: JoinType },
    NestedLoopJoin { left: Box<PlanNode>, right: Box<PlanNode>, join_key: String, join_type: JoinType },
    SortMergeJoin { left: Box<PlanNode>, right: Box<PlanNode>, join_key: String, join_type: JoinType },
    Aggregate { input: Box<PlanNode>, group_by: Vec<String> },
    Sort { input: Box<PlanNode>, order_by: Vec<String> },
    Limit { input: Box<PlanNode>, count: usize },
    Subquery { input: Box<PlanNode>, correlated: bool },
    Union { left: Box<PlanNode>, right: Box<PlanNode> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType { Inner, Left, Right, Full }

impl PlanNode {
    pub fn estimated_cost(&self) -> f64 {
        match self {
            PlanNode::SeqScan { estimated_cost, .. } => *estimated_cost,
            PlanNode::IndexScan { estimated_cost, .. } => *estimated_cost,
            PlanNode::Filter { input, selectivity, .. } => input.estimated_cost() * selectivity,
            PlanNode::HashJoin { left, right, join_type, .. } => {
                let cost = left.estimated_cost() + right.estimated_cost() * 2.0;
                match join_type {
                    JoinType::Inner | JoinType::Left | JoinType::Right => cost,
                    JoinType::Full => cost * 1.5,
                }
            }
            PlanNode::NestedLoopJoin { left, right, .. } => left.estimated_cost() * right.estimated_cost(),
            PlanNode::SortMergeJoin { left, right, .. } => left.estimated_cost() + right.estimated_cost() + 100.0,
            PlanNode::Aggregate { input, .. } => input.estimated_cost() * 1.5,
            PlanNode::Sort { input, .. } => input.estimated_cost() * 2.0,
            PlanNode::Limit { input, .. } => input.estimated_cost() * 0.1,
            PlanNode::Subquery { input, .. } => input.estimated_cost() * 3.0,
            PlanNode::Union { left, right } => left.estimated_cost() + right.estimated_cost(),
        }
    }

    pub fn estimated_rows(&self) -> u64 {
        match self {
            PlanNode::SeqScan { estimated_rows, .. } => *estimated_rows,
            PlanNode::IndexScan { estimated_rows, .. } => *estimated_rows,
            PlanNode::Filter { input, selectivity, .. } => (input.estimated_rows() as f64 * selectivity) as u64,
            PlanNode::HashJoin { left, right, join_type, .. } => {
                match join_type {
                    JoinType::Inner => (left.estimated_rows() * right.estimated_rows() / 10).max(1),
                    JoinType::Left | JoinType::Right => left.estimated_rows().max(right.estimated_rows()),
                    JoinType::Full => left.estimated_rows() + right.estimated_rows(),
                }
            }
            PlanNode::Aggregate { input, group_by } => {
                if group_by.is_empty() { 1 } else { input.estimated_rows() / 10 }
            }
            PlanNode::Sort { input, .. } => input.estimated_rows(),
            PlanNode::Limit { input, count } => (*count as u64).min(input.estimated_rows()),
            PlanNode::Subquery { input, .. } => input.estimated_rows(),
            PlanNode::Union { left, right } => left.estimated_rows() + right.estimated_rows(),
            _ => 1000,
        }
    }
}

/// Table statistics for cost estimation
#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: u64,
    pub avg_row_size: u64,
    pub column_stats: HashMap<String, ColumnStats>,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_count: u64,
    pub null_count: u64,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

/// Query optimizer
pub struct QueryOptimizer {
    stats: HashMap<String, TableStats>,
}

impl QueryOptimizer {
    pub fn new() -> Self { Self { stats: HashMap::new() } }

    pub fn update_stats(&mut self, table: String, stats: TableStats) {
        self.stats.insert(table, stats);
    }

    /// Choose between seq scan and index scan
    pub fn choose_scan(&self, table: &str, has_index: bool, predicate_selectivity: f64) -> PlanNode {
        let stats = self.stats.get(table);
        let seq_cost = stats.map(|s| s.row_count as f64 * s.avg_row_size as f64 / 1024.0).unwrap_or(1000.0);
        let idx_cost = if has_index { seq_cost * predicate_selectivity * 0.1 } else { f64::MAX };
        let estimated_rows = stats.map(|s| (s.row_count as f64 * predicate_selectivity) as u64).unwrap_or(100);

        if idx_cost < seq_cost {
            PlanNode::IndexScan { table: table.to_string(), index: format!("idx_{}", table), estimated_rows, estimated_cost: idx_cost }
        } else {
            PlanNode::SeqScan { table: table.to_string(), estimated_rows, estimated_cost: seq_cost }
        }
    }

    /// Rewrite join order based on cost
    pub fn rewrite_join_order(&self, left_table: &str, right_table: &str, join_key: &str, join_type: JoinType) -> PlanNode {
        let left_rows = self.stats.get(left_table).map(|s| s.row_count).unwrap_or(1000);
        let right_rows = self.stats.get(right_table).map(|s| s.row_count).unwrap_or(1000);

        let (small_table, large_table) = if left_rows <= right_rows {
            (left_table, right_table)
        } else {
            (right_table, left_table)
        };

        let small_rows = left_rows.min(right_rows);
        let large_rows = left_rows.max(right_rows);

        // Choose join algorithm based on table sizes
        let left_node = PlanNode::SeqScan {
            table: small_table.to_string(),
            estimated_rows: small_rows,
            estimated_cost: small_rows as f64,
        };
        let right_node = PlanNode::SeqScan {
            table: large_table.to_string(),
            estimated_rows: large_rows,
            estimated_cost: large_rows as f64,
        };

        // Hash join for inner joins when one table is small
        if join_type == JoinType::Inner && small_rows < 10000 {
            PlanNode::HashJoin { left: Box::new(left_node), right: Box::new(right_node), join_key: join_key.to_string(), join_type }
        } else {
            PlanNode::SortMergeJoin { left: Box::new(left_node), right: Box::new(right_node), join_key: join_key.to_string(), join_type }
        }
    }

    /// Optimize subquery
    pub fn optimize_subquery(&self, inner: PlanNode, _outer_table: &str) -> PlanNode {
        // If subquery is not correlated, can be evaluated once
        PlanNode::Subquery { input: Box::new(inner), correlated: false }
    }

    /// Estimate predicate selectivity
    pub fn estimate_selectivity(&self, table: &str, column: &str, op: &str) -> f64 {
        let stats = self.stats.get(table);
        match op {
            "=" => {
                if let Some(s) = stats {
                    if let Some(col_stats) = s.column_stats.get(column) {
                        return 1.0 / col_stats.distinct_count as f64;
                    }
                }
                0.1
            }
            ">" | "<" | ">=" | "<=" => 0.3,
            "LIKE" => 0.05,
            _ => 0.1,
        }
    }

    /// Estimate join selectivity
    pub fn estimate_join_selectivity(&self, left_table: &str, right_table: &str, join_key: &str) -> f64 {
        let left_distinct = self.stats.get(left_table)
            .and_then(|s| s.column_stats.get(join_key))
            .map(|c| c.distinct_count)
            .unwrap_or(1000);
        let right_distinct = self.stats.get(right_table)
            .and_then(|s| s.column_stats.get(join_key))
            .map(|c| c.distinct_count)
            .unwrap_or(1000);

        1.0 / left_distinct.max(right_distinct) as f64
    }
}

impl Default for QueryOptimizer {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimizer_scan_choice() {
        let mut opt = QueryOptimizer::new();
        opt.update_stats("test".to_string(), TableStats {
            row_count: 1000000, avg_row_size: 100, column_stats: HashMap::new(),
        });
        let plan = opt.choose_scan("test", true, 0.1);
        match plan {
            PlanNode::IndexScan { .. } => {}
            _ => panic!("expected index scan"),
        }
        let plan = opt.choose_scan("test", false, 0.1);
        match plan {
            PlanNode::SeqScan { .. } => {}
            _ => panic!("expected seq scan"),
        }
    }

    #[test]
    fn test_optimizer_join_rewrite() {
        let mut opt = QueryOptimizer::new();
        opt.update_stats("big".to_string(), TableStats { row_count: 1000000, avg_row_size: 100, column_stats: HashMap::new() });
        opt.update_stats("small".to_string(), TableStats { row_count: 100, avg_row_size: 50, column_stats: HashMap::new() });
        let plan = opt.rewrite_join_order("big", "small", "id", JoinType::Inner);
        match plan {
            PlanNode::HashJoin { left, right, .. } => {
                match *left {
                    PlanNode::SeqScan { table, .. } => assert_eq!(table, "small"),
                    _ => panic!("expected small table first"),
                }
                match *right {
                    PlanNode::SeqScan { table, .. } => assert_eq!(table, "big"),
                    _ => panic!("expected big table second"),
                }
            }
            _ => panic!("expected hash join"),
        }
    }

    #[test]
    fn test_optimizer_selectivity() {
        let mut opt = QueryOptimizer::new();
        opt.update_stats("test".to_string(), TableStats {
            row_count: 1000000, avg_row_size: 100,
            column_stats: vec![("id".to_string(), ColumnStats { distinct_count: 1000000, null_count: 0, min_value: None, max_value: None })].into_iter().collect(),
        });
        assert!((opt.estimate_selectivity("test", "id", "=") - 0.000001).abs() < 0.001);
        assert!((opt.estimate_selectivity("test", "id", ">") - 0.3).abs() < 0.01);
    }
}
