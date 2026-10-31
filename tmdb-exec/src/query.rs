//! Single-table SELECT execution: projection, filtering, aggregation,
//! `GROUP BY` (including time buckets), `ORDER BY` and `LIMIT`.
//!
//! This replaces the previous string-slicing implementation, which found `FROM`
//! and ` WHERE ` with `str::find`, ignored the projection list and every
//! non-time predicate, and returned a full-table scan. A query against it
//! produced plausible rows that were simply not the rows that were asked for.
//!
//! Everything here works off the `sqlparser` AST, so `tmdb-exec` and
//! `tmdb-sql` cannot disagree about what a statement means.

use std::cmp::Ordering;

use tmdb_timestore::{ColumnDef, DataType, Table, TableMeta};
use sqlparser::ast::{
    BinaryOperator, Distinct, Expr, FunctionArgExpr, FunctionArguments, GroupByExpr, Query,
    SelectItem, SetExpr, Value as SqlValue,
};

use crate::values::Value;

/// Aggregate functions this engine evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
}

impl AggFunc {
    fn parse(name: &str) -> Option<AggFunc> {
        Some(match name.to_ascii_lowercase().as_str() {
            "count" => AggFunc::Count,
            "sum" | "total" => AggFunc::Sum,
            "avg" | "mean" => AggFunc::Avg,
            "min" => AggFunc::Min,
            "max" => AggFunc::Max,
            "first" => AggFunc::First,
            "last" => AggFunc::Last,
            _ => return None,
        })
    }
}

/// One aggregate in the projection.
#[derive(Debug, Clone)]
struct Aggregate {
    func: AggFunc,
    /// `None` for `count(*)`.
    arg: Option<Expr>,
}

/// One `GROUP BY` term.
#[derive(Debug, Clone)]
enum GroupTerm {
    /// `time(<interval>)` or `time(<interval>, <offset>)`.
    TimeBucket { interval: i64, offset: i64 },
    /// A tag or value column, grouped by exact value.
    Column { index: usize },
}

/// One output column.
struct OutputColumn {
    name: String,
    data_type: tmdb_timestore::DataType,
    /// How to produce this column's value for one row (non-aggregate path).
    expr: Option<Expr>,
    /// Set for group-by columns, whose value comes from the group key.
    from_group: Option<usize>,
    /// Set for aggregate columns.
    aggregate: Option<usize>,
}

/// The planned output of a SELECT: the columns plus the aggregate definitions
/// they refer to. These have to travel together, because an output column only
/// stores the *index* of its aggregate.
struct ResolvedSelect {
    columns: Vec<OutputColumn>,
    aggregates: Vec<Aggregate>,
}

/// One scanned row plus the schema needed to interpret it.
///
/// Two addressing modes share this type. In the first, `values` are the raw
/// table columns and names resolve against `meta`. In the second, `values` are
/// already-projected output rows — the state ORDER BY and HAVING both run in —
/// and names resolve against `output` instead, which may be an alias, an
/// ordinal, or an aggregate the SELECT list just produced.
struct RowCtx<'a> {
    values: &'a [Option<Vec<u8>>],
    meta: &'a TableMeta,
    /// Output columns, when `values` is an output row rather than a table row.
    output: Option<&'a [OutputColumn]>,
    /// Aggregate signature to `(column index, type)` in `values`.
    ///
    /// HAVING may name an aggregate that the SELECT list does not project, so
    /// those are materialised as hidden output columns and dropped afterwards.
    aggregates: Option<&'a std::collections::HashMap<String, (usize, DataType)>>,
}

impl<'a> RowCtx<'a> {
    /// Resolve a name against the table schema.
    fn column(&self, name: &str) -> Option<(usize, &ColumnDef)> {
        // `time` and `ts` both address the timestamp column, because a time
        // series query should not have to remember which one the DDL used.
        let lowered = name.to_ascii_lowercase();
        self.meta.columns.iter().enumerate().find_map(|(i, c)| {
            let matches = c.name.eq_ignore_ascii_case(name)
                || (matches!(c.data_type, DataType::Timestamp)
                    && (lowered == "time" || lowered == "ts"));
            matches.then_some((i, c))
        })
    }

    /// Resolve a name against the projected output, if this row is one.
    fn resolve_output(&self, name: &str) -> Option<(usize, DataType)> {
        let columns = self.output?;
        let idx = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))?;
        Some((idx, columns[idx].data_type))
    }
}

/// Canonical signature for an aggregate, used to match an `HAVING` reference
/// against the column that already holds its value.
///
/// Built from the function name and the argument's display form so that the
/// same call in the SELECT list and in HAVING produce the same key.
fn aggregate_key(func: &AggFunc, arg: &Option<Expr>) -> String {
    let name = match func {
        AggFunc::Count => "count",
        AggFunc::Sum => "sum",
        AggFunc::Avg => "avg",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
        AggFunc::First => "first",
        AggFunc::Last => "last",
    };
    match arg {
        None => format!("{}(*)", name),
        Some(e) => format!("{}({})", name, e.to_string().to_ascii_lowercase()),
    }
}

/// Evaluate a scalar expression against one row.
fn eval_expr(expr: &Expr, row: &RowCtx<'_>, ts: i64) -> anyhow::Result<Value> {
    match expr {
        Expr::Identifier(ident) => {
            // An output row may carry a column the table does not have: an
            // alias, an aggregate, or a GROUP BY term. Those must resolve
            // before the table schema, or `HAVING count(*) > 5` looks up a
            // column that was never written.
            if let Some((i, ty)) = row.resolve_output(&ident.value) {
                return Ok(Value::decode(&ty, row.values.get(i).and_then(|v| v.as_deref())));
            }
            if let Some((i, col)) = row.column(&ident.value) {
                Ok(Value::decode(&col.data_type, row.values.get(i).and_then(|v| v.as_deref())))
            } else {
                anyhow::bail!("unknown column '{}'", ident.value)
            }
        }
        Expr::CompoundIdentifier(parts) => {
            let name = parts.last().map(|p| p.value.as_str()).unwrap_or_default();
            if let Some((i, ty)) = row.resolve_output(name) {
                return Ok(Value::decode(&ty, row.values.get(i).and_then(|v| v.as_deref())));
            }
            if let Some((i, col)) = row.column(name) {
                Ok(Value::decode(&col.data_type, row.values.get(i).and_then(|v| v.as_deref())))
            } else {
                anyhow::bail!("unknown column '{}'", name)
            }
        }
        // An aggregate already materialised in the row. `as_aggregate` needs
        // the table schema to type the argument, so that stays `row.meta`.
        Expr::Function(_) => {
            let Some(map) = row.aggregates else {
                anyhow::bail!(
                    "aggregate function '{}' is not in the SELECT list or GROUP BY",
                    expr
                );
            };
            let Some((func, arg, _)) = as_aggregate(expr, row.meta)? else {
                anyhow::bail!("'{}' is not an aggregate function", expr);
            };
            let key = aggregate_key(&func, &arg);
            let Some((i, ty)) = map.get(&key) else {
                anyhow::bail!("aggregate '{}' is not available after aggregation", expr);
            };
            Ok(Value::decode(ty, row.values.get(*i).and_then(|v| v.as_deref())))
        }
        Expr::Value(v) => Ok(literal(v)),
        Expr::Nested(inner) => eval_expr(inner, row, ts),
        Expr::IsNull(inner) => Ok(Value::Bool(eval_expr(inner, row, ts)?.is_null())),
        Expr::IsNotNull(inner) => Ok(Value::Bool(!eval_expr(inner, row, ts)?.is_null())),
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, row, ts)?;
            match op {
                sqlparser::ast::UnaryOperator::Not => Ok(match v {
                    Value::Null => Value::Null,
                    Value::Bool(b) => Value::Bool(!b),
                    other => anyhow::bail!("NOT applied to a non-boolean {:?}", other),
                }),
                sqlparser::ast::UnaryOperator::Minus => Ok(match v {
                    Value::Int(i) => Value::Int(-i),
                    Value::Float(f) => Value::Float(-f),
                    other => anyhow::bail!("cannot negate {:?}", other),
                }),
                sqlparser::ast::UnaryOperator::Plus => Ok(v),
                other => anyhow::bail!("unsupported unary operator {:?}", other),
            }
        }
        Expr::BinaryOp { left, op, right } => eval_binary(left, op, right, row, ts),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let v = eval_expr(expr, row, ts)?;
            let lo = eval_expr(low, row, ts)?;
            let hi = eval_expr(high, row, ts)?;
            // A NULL bound makes the whole test unknown, as in SQL.
            let inside = match (v.compare(&lo), v.compare(&hi)) {
                (Some(a), Some(b)) => a != Ordering::Greater && b != Ordering::Less,
                _ => return Ok(Value::Null),
            };
            Ok(Value::Bool(if *negated { !inside } else { inside }))
        }
        Expr::InList { expr, list, negated } => {
            let v = eval_expr(expr, row, ts)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            let mut saw_null = false;
            let mut found = false;
            for item in list {
                match eval_expr(item, row, ts)? {
                    Value::Null => saw_null = true,
                    other => {
                        if v.compare(&other) == Some(Ordering::Equal) {
                            found = true;
                        }
                    }
                }
            }
            if !found && saw_null {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(if *negated { !found } else { found }))
        }
        Expr::Like { expr, pattern, negated, .. } => {
            let v = eval_expr(expr, row, ts)?;
            let p = eval_expr(pattern, row, ts)?;
            match (v, p) {
                (Value::Str(s), Value::Str(pat)) => {
                    let hit = like_matches(&s, &pat);
                    Ok(Value::Bool(if *negated { !hit } else { hit }))
                }
                _ => Ok(Value::Null),
            }
        }
        other => anyhow::bail!("unsupported expression: {}", other),
    }
}

fn eval_binary(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    row: &RowCtx<'_>,
    ts: i64,
) -> anyhow::Result<Value> {
    // AND/OR short-circuit on the unknown case the way SQL requires.
    match op {
        BinaryOperator::And => {
            let l = eval_expr(left, row, ts)?;
            if l == Value::Bool(false) {
                return Ok(Value::Bool(false));
            }
            let r = eval_expr(right, row, ts)?;
            if r == Value::Bool(false) {
                return Ok(Value::Bool(false));
            }
            return Ok(match (l, r) {
                (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
                _ => Value::Null,
            });
        }
        BinaryOperator::Or => {
            let l = eval_expr(left, row, ts)?;
            if l == Value::Bool(true) {
                return Ok(Value::Bool(true));
            }
            let r = eval_expr(right, row, ts)?;
            if r == Value::Bool(true) {
                return Ok(Value::Bool(true));
            }
            return Ok(match (l, r) {
                (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
                _ => Value::Null,
            });
        }
        _ => {}
    }

    let l = eval_expr(left, row, ts)?;
    let r = eval_expr(right, row, ts)?;

    // Arithmetic on two values, with NULL propagating.
    match op {
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => {
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }
            // Keep integers integral when both sides are integral, except for
            // division where SQL yields a real.
            if let (Value::Int(a), Value::Int(b)) = (&l, &r) {
                let (a, b) = (*a, *b);
                return Ok(match op {
                    BinaryOperator::Plus => Value::Int(a.wrapping_add(b)),
                    BinaryOperator::Minus => Value::Int(a.wrapping_sub(b)),
                    BinaryOperator::Multiply => Value::Int(a.wrapping_mul(b)),
                    BinaryOperator::Divide => {
                        if b == 0 {
                            return Ok(Value::Null);
                        }
                        Value::Float(a as f64 / b as f64)
                    }
                    BinaryOperator::Modulo => {
                        if b == 0 {
                            return Ok(Value::Null);
                        }
                        Value::Int(a % b)
                    }
                    _ => unreachable!(),
                });
            }
            let (a, b) = match (l.as_f64(), r.as_f64()) {
                (Some(a), Some(b)) => (a, b),
                _ => return Ok(Value::Null),
            };
            return Ok(match op {
                BinaryOperator::Plus => Value::Float(a + b),
                BinaryOperator::Minus => Value::Float(a - b),
                BinaryOperator::Multiply => Value::Float(a * b),
                BinaryOperator::Divide => {
                    if b == 0.0 {
                        Value::Null
                    } else {
                        Value::Float(a / b)
                    }
                }
                BinaryOperator::Modulo => {
                    if b == 0.0 {
                        Value::Null
                    } else {
                        Value::Float(a % b)
                    }
                }
                _ => unreachable!(),
            });
        }
        _ => {}
    }

    // Logical operators over already-decoded values.
    match op {
        BinaryOperator::And | BinaryOperator::Or => unreachable!("handled above"),
        _ => {}
    }

    let cmp = l.compare(&r);
    // A comparison involving NULL is unknown, which a WHERE then treats as
    // false, so the tri-state collapses to a plain NULL value here.
    let boolean = match op {
        BinaryOperator::Eq => cmp.map(|o| o == Ordering::Equal),
        BinaryOperator::NotEq => cmp.map(|o| o != Ordering::Equal),
        BinaryOperator::Lt => cmp.map(|o| o == Ordering::Less),
        BinaryOperator::LtEq => cmp.map(|o| o != Ordering::Greater),
        BinaryOperator::Gt => cmp.map(|o| o == Ordering::Greater),
        BinaryOperator::GtEq => cmp.map(|o| o != Ordering::Less),
        other => return Err(anyhow::anyhow!("unsupported operator {:?}", other)),
    };
    Ok(boolean.map(Value::Bool).unwrap_or(Value::Null))
}

fn literal(v: &SqlValue) -> Value {
    match v {
        SqlValue::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Value::Int(i)
            } else if let Ok(f) = n.parse::<f64>() {
                Value::Float(f)
            } else {
                Value::Str(n.clone())
            }
        }
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::EscapedStringLiteral(s)
        | SqlValue::NationalStringLiteral(s) => Value::Str(s.clone()),
        SqlValue::Boolean(b) => Value::Bool(*b),
        SqlValue::Null => Value::Null,
        other => Value::Str(other.to_string()),
    }
}

/// SQL `LIKE` with `%` and `_`. Only reachable for string columns.
fn like_matches(s: &str, pattern: &str) -> bool {
    fn helper(s: &[char], p: &[char]) -> bool {
        if p.is_empty() {
            return s.is_empty();
        }
        match p[0] {
            '%' => {
                // Collapse runs of '%' so `a%%b` does not blow up.
                let rest = if p.len() > 1 && p[1] == '%' {
                    helper(s, &p[1..])
                } else {
                    (0..=s.len()).any(|i| helper(&s[i..], &p[1..]))
                };
                rest
            }
            '_' => !s.is_empty() && helper(&s[1..], &p[1..]),
            c => !s.is_empty() && s[0] == c && helper(&s[1..], &p[1..]),
        }
    }
    let sc: Vec<char> = s.chars().collect();
    let pc: Vec<char> = pattern.chars().collect();
    helper(&sc, &pc)
}

/// Running state for one aggregate within one group.
#[derive(Debug, Clone)]
struct AggState {
    count: u64,
    /// Number of non-null values seen, which is `sum`'s divisor for `avg`
    /// and the result of `count(col)`.
    non_null: u64,
    sum: f64,
    min: Option<Value>,
    max: Option<Value>,
    first: Option<Value>,
    last: Option<Value>,
}

/// Execute a single-table SELECT.
///
/// Returns the output column names, their types, and the encoded values. The
/// types travel with the result because a consumer cannot recover them from the
/// bytes: an 8-byte column could be a timestamp, a bigint or a double, and
/// guessing produces confidently wrong numbers.
pub fn execute_select(
    table: &Table,
    query: &Query,
) -> anyhow::Result<(Vec<String>, Vec<tmdb_timestore::DataType>, Vec<Vec<Option<Vec<u8>>>>)>
{
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        SetExpr::Query(_) => anyhow::bail!("nested queries are not supported"),
        SetExpr::SetOperation { op, .. } => {
            anyhow::bail!("{} is not supported; run one SELECT at a time", op)
        }
        other => anyhow::bail!("unsupported query body: {}", other),
    };

    if select.from.len() != 1 {
        anyhow::bail!(
            "expected exactly one table in FROM, found {}",
            select.from.len()
        );
    }
    if !select.from[0].joins.is_empty() {
        anyhow::bail!("JOIN is not supported yet");
    }
    let table_name = match &select.from[0].relation {
        sqlparser::ast::TableFactor::Table { name, .. } => name
            .0
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_default(),
        other => anyhow::bail!("unsupported table factor: {}", other),
    };

    let meta = &table.meta;
    // The caller picks the table by name, so a mismatch means the plan and the
    // data disagree. Reading table `b` for `FROM a` would return plausible rows
    // for the wrong series, so refuse instead.
    if !table_name.is_empty() && !table_name.eq_ignore_ascii_case(&meta.name) {
        anyhow::bail!(
            "query reads '{}' but was planned against table '{}'",
            table_name,
            meta.name
        );
    }
    let ts_col = meta
        .ts_column()
        .map(|c| c.name.clone())
        .unwrap_or_else(|| "time".to_string());

// Resolve the schema before touching data, so an unknown column is an error
    // rather than a silently empty result.
    let resolved = build_output(select, meta)?;
    let mut output_columns = resolved.columns;
    let mut aggregates = resolved.aggregates;
    let group_terms = build_group_by(select, meta)?;

    // HAVING may name an aggregate the SELECT list does not project. Those are
    // materialised as extra output columns so they can be evaluated, and are
    // dropped again before the result is returned.
    let mut hidden = 0usize;
    if let Some(having) = select.having.as_ref() {
        let mut extra = Vec::new();
        collect_having_aggregates(having, meta, &mut extra)?;
        for agg in extra {
            let key = aggregate_key(&agg.func, &agg.arg);
            if aggregates.iter().any(|a| aggregate_key(&a.func, &a.arg) == key) {
                continue;
            }
            let index = aggregates.len();
            let data_type = match agg.func {
                AggFunc::Count => DataType::Int64,
                AggFunc::Sum | AggFunc::Avg => DataType::Float64,
                _ => match &agg.arg {
                    Some(Expr::Identifier(ident)) => resolve(meta, &ident.value)?.1.data_type,
                    _ => DataType::Float64,
                },
            };
            aggregates.push(agg);
            output_columns.push(OutputColumn {
                name: format!("__having_{}", hidden),
                data_type,
                expr: None,
                from_group: None,
                aggregate: Some(index),
            });
            hidden += 1;
        }
    }

    let visible_columns = output_columns.len() - hidden;

    // Canonical aggregate signature to the output column that holds its value.
    let mut agg_map: std::collections::HashMap<String, (usize, DataType)> =
        std::collections::HashMap::new();
    for (idx, c) in output_columns.iter().enumerate() {
        if let Some(ai) = c.aggregate {
            if let Some(agg) = aggregates.get(ai) {
                agg_map.insert(aggregate_key(&agg.func, &agg.arg), (idx, c.data_type));
            }
        }
    }

    // Check HAVING before touching any data. A table with no rows never reaches
    // the evaluator, so an unresolvable name would otherwise come back as an
    // empty result rather than as an error.
    if let Some(having) = select.having.as_ref() {
        validate_having(having, &output_columns, &agg_map, meta)?;
    }

    let aggregating = !aggregates.is_empty() || !group_terms.is_empty();

    if aggregating {
        for c in &output_columns[..visible_columns] {
            if c.expr.is_some() {
                anyhow::bail!(
                    "column '{}' must appear in GROUP BY or be wrapped in an aggregate",
                    c.name
                );
            }
        }
    }

    // Push a time predicate into the scan so segment and block pruning can do
    // their job. The same predicate is still evaluated per row below; this only
    // narrows what has to be read.
    let (ts_min, ts_max) = time_bounds(select.selection.as_ref(), meta, &ts_col);

    let mut scanned = table.scan(ts_min, ts_max, &[], None)?;
    scanned.sort_by_key(|r| r.0);

    // The filter that was previously dropped entirely.
    let filtered: Vec<(i64, Vec<Option<Vec<u8>>>)> = match &select.selection {
        None => scanned,
        Some(pred) => {
                        let mut kept = Vec::with_capacity(scanned.len());
            for (ts, values) in scanned {
                let row = RowCtx {
                    values: &values,
                    meta,
                    output: None,
                    aggregates: None,
                };
                if eval_expr(pred, &row, ts)? == Value::Bool(true) {
                    kept.push((ts, values));
                }
            }
            kept
        }
    };

let mut out_rows: Vec<Vec<Option<Vec<u8>>>> = if aggregating {
        aggregate_rows(&filtered, meta, &output_columns, &group_terms, &aggregates)?
    } else {
        project_rows(&filtered, meta, &output_columns)?
    };

    // HAVING runs after aggregation and before ORDER BY, so a group that fails
    // it never reaches the sort or the LIMIT.
    if let Some(having) = select.having.as_ref() {
        out_rows = filter_having(out_rows, having, &output_columns, meta, &agg_map, 0)?;
    }

    // DISTINCT applies to the projected result, after HAVING.
    if let Some(distinct) = select.distinct.as_ref() {
        match distinct {
            Distinct::Distinct => out_rows = distinct_rows(out_rows),
            Distinct::On(_) => anyhow::bail!("DISTINCT ON is not supported yet"),
        }
    }

    // Drop the columns that only existed to evaluate HAVING, so they never
    // appear in the result.
    if hidden > 0 {
        for row in &mut out_rows {
            row.truncate(visible_columns);
        }
        output_columns.truncate(visible_columns);
    }

    // ORDER BY is evaluated against the output, so it can name a column alias,
    // an ordinal, or the aggregate it just produced.
    if let Some(order_by) = query.order_by.as_ref() {
        sort_rows(&mut out_rows, &output_columns, &order_by.exprs)?;
    }
    if let Some(limit) = query.limit.as_ref() {
        match limit_as_count(limit)? {
            Some(n) => out_rows.truncate(n),
            None => anyhow::bail!("LIMIT needs a literal row count, got {}", limit),
        }
    }

    Ok((
        output_columns.iter().map(|c| c.name.clone()).collect(),
        output_columns.iter().map(|c| c.data_type).collect(),
        out_rows,
    ))
}

/// Read a `LIMIT`/`OFFSET` count. `ALL` means no bound.
fn limit_as_count(expr: &Expr) -> anyhow::Result<Option<usize>> {
    match expr {
        Expr::Value(SqlValue::Number(n, _)) => n
            .parse::<usize>()
            .map(Some)
            .map_err(|_| anyhow::anyhow!("'{}' is not a valid row count", n)),
        other => Ok(match literal_of(other) {
            Value::Null => None,
            v => Some(v.as_f64().ok_or_else(|| {
                anyhow::anyhow!("'{}' is not a valid row count", v.to_text())
            })? as usize),
        }),
    }
}

/// Fold a constant expression down to a literal value, for LIMIT and similar.
fn literal_of(expr: &Expr) -> Value {
    match expr {
        Expr::Value(v) => literal(v),
        Expr::Nested(inner) => literal_of(inner),
        _ => Value::Null,
    }
}

fn project_rows(
    rows: &[(i64, Vec<Option<Vec<u8>>>)],
    meta: &TableMeta,
    columns: &[OutputColumn],
) -> anyhow::Result<Vec<Vec<Option<Vec<u8>>>>> {
    let mut out = Vec::with_capacity(rows.len());
    for (ts, values) in rows {
        let row = RowCtx {
            values,
            meta,
            output: None,
            aggregates: None,
        };
        let mut encoded = Vec::with_capacity(columns.len());
        for c in columns {
            let v = match &c.expr {
                Some(e) => eval_expr(e, &row, *ts)?,
                None => Value::Null,
            };
            encoded.push(v.encode(&c.data_type)?);
        }
        out.push(encoded);
    }
    Ok(out)
}

/// Mutable group accumulator keyed by the encoded group key.
fn aggregate_rows(
    rows: &[(i64, Vec<Option<Vec<u8>>>)],
    meta: &TableMeta,
    columns: &[OutputColumn],
    group_terms: &[GroupTerm],
    aggregates: &[Aggregate],
) -> anyhow::Result<Vec<Vec<Option<Vec<u8>>>>> {
    // Preserve first-seen group order rather than hashing into an unordered map,
    // so output is stable and reproducible.
    let mut order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<String, (Vec<Value>, Vec<AggState>)> =
        std::collections::HashMap::new();

    for (seq, (ts, values)) in rows.iter().enumerate() {
        let row = RowCtx {
            values,
            meta,
            output: None,
            aggregates: None,
        };
        let _ = seq;

        let mut key_values: Vec<Value> = Vec::with_capacity(group_terms.len());
        for term in group_terms {
            key_values.push(match term {
                GroupTerm::TimeBucket { interval, offset } => {
                    Value::Int(bucket_start(*ts, *interval, *offset))
                }
                GroupTerm::Column { index } => Value::decode(
                    &meta.columns[*index].data_type,
                    values.get(*index).and_then(|v| v.as_deref()),
                ),
            });
        }
        let key = group_key(&key_values);

        let entry = groups.entry(key.clone()).or_insert_with(|| {
            order.push(key.clone());
            (
                key_values.clone(),
                vec![
                    AggState {
                        count: 0,
                        non_null: 0,
                        sum: 0.0,
                        min: None,
                        max: None,
                        first: None,
                        last: None,
                    };
                    aggregates.len()
                ],
            )
        });

        for (i, agg) in aggregates.iter().enumerate() {
            let value = match &agg.arg {
                None => Value::Int(1),
                Some(e) => eval_expr(e, &row, *ts)?,
            };
            accumulate(&mut entry.1[i], &value);
        }
    }

    // A query with aggregates but no GROUP BY still produces exactly one row,
    // even over an empty input -- `SELECT count(*) FROM t` on an empty table is
    // 0, not zero rows.
    if group_terms.is_empty() && groups.is_empty() {
        order.push(String::new());
        groups.insert(
            String::new(),
            (
                Vec::new(),
                vec![
                    AggState {
                        count: 0,
                        non_null: 0,
                        sum: 0.0,
                        min: None,
                        max: None,
                        first: None,
                        last: None,
                    };
                    aggregates.len()
                ],
            ),
        );
    }

    let mut out = Vec::with_capacity(order.len());
    for key in &order {
        let (key_values, states) = &groups[key];
        let mut encoded = Vec::with_capacity(columns.len());
        for c in columns {
            let v = if let Some(gi) = c.from_group {
                key_values.get(gi).cloned().unwrap_or(Value::Null)
            } else if let Some(ai) = c.aggregate {
                let agg = &aggregates[ai];
                finish(&states[ai], agg)
            } else {
                Value::Null
            };
            encoded.push(v.encode(&c.data_type)?);
        }
        out.push(encoded);
    }
    Ok(out)
}

fn group_key(values: &[Value]) -> String {
    // Values of different types must never share a group, and a value must never
    // be able to forge its neighbours' key, so every part is length-prefixed and
    // tagged with its type. Comparing raw text alone would merge the integer 1
    // with the string "1".
    let mut s = String::new();
    for v in values {
        let (tag, text) = match v {
            Value::Null => ("N", String::new()),
            Value::Bool(b) => ("B", b.to_string()),
            Value::Int(i) => ("I", i.to_string()),
            Value::Float(f) => ("F", f.to_string()),
            Value::Str(t) => ("T", t.clone()),
        };
        s.push_str(tag);
        s.push(':');
        s.push_str(&text.len().to_string());
        s.push(':');
        s.push_str(&text);
    }
    s
}

fn accumulate(state: &mut AggState, value: &Value) {
    state.count += 1;
    if !value.is_null() {
        state.non_null += 1;
        if let Some(n) = value.as_f64() {
            state.sum += n;
        }
        state.first.get_or_insert_with(|| value.clone());
        state.last = Some(value.clone());
        // min/max keep the value so a string column aggregates lexically, which
        // is what a user asking for MIN on text expects.
        match &state.min {
            None => state.min = Some(value.clone()),
            Some(cur) => {
                if value.compare(cur) == Some(Ordering::Less) {
                    state.min = Some(value.clone());
                }
            }
        }
        match &state.max {
            None => state.max = Some(value.clone()),
            Some(cur) => {
                if value.compare(cur) == Some(Ordering::Greater) {
                    state.max = Some(value.clone());
                }
            }
        }
    }
}

fn finish(state: &AggState, agg: &Aggregate) -> Value {
    match agg.func {
        AggFunc::Count => match &agg.arg {
            // count(*) counts rows, including those whose value column is NULL.
            None => Value::Int(state.count as i64),
            Some(_) => Value::Int(state.non_null as i64),
        },
        AggFunc::Sum => {
            if state.non_null == 0 {
                Value::Null
            } else {
                Value::Float(state.sum)
            }
        }
        AggFunc::Avg => {
            if state.non_null == 0 {
                Value::Null
            } else {
                Value::Float(state.sum / state.non_null as f64)
            }
        }
        AggFunc::Min => state.min.clone().unwrap_or(Value::Null),
        AggFunc::Max => state.max.clone().unwrap_or(Value::Null),
        AggFunc::First => state.first.clone().unwrap_or(Value::Null),
        AggFunc::Last => state.last.clone().unwrap_or(Value::Null),
    }
}

fn bucket_start(ts: i64, interval: i64, offset: i64) -> i64 {
    if interval <= 0 {
        return ts + offset;
    }
    // Floor division, not `%`, so buckets stay aligned for negative timestamps
    // (a pre-1970 point must not land in bucket -1..0 while its neighbour at +1
    // lands in 0..interval).
    ((ts - offset).div_euclid(interval)) * interval + offset
}

/// Pull a `time` predicate out of the WHERE clause for scan pruning.
fn time_bounds(selection: Option<&Expr>, meta: &TableMeta, ts_col: &str) -> (i64, i64) {
    let mut min = i64::MIN;
    let mut max = i64::MAX;
    let is_ts = |e: &Expr| match e {
        Expr::Identifier(i) => i.value.eq_ignore_ascii_case(ts_col) || i.value.eq_ignore_ascii_case("ts"),
        _ => false,
    };

    fn visit(e: &Expr, is_ts: &dyn Fn(&Expr) -> bool, min: &mut i64, max: &mut i64) {
        match e {
            Expr::BinaryOp { left, op, right } if matches!(op, BinaryOperator::And) => {
                visit(left, is_ts, min, max);
                visit(right, is_ts, min, max);
            }
            Expr::Nested(inner) => visit(inner, is_ts, min, max),
            Expr::BinaryOp { left, op, right } if is_ts(left) => {
                if let Expr::Value(v) = right.as_ref() {
                    match literal(v) {
                        Value::Int(n) => match op {
                            BinaryOperator::Gt => *min = (*min).max(n),
                            BinaryOperator::GtEq => *min = (*min).max(n),
                            BinaryOperator::Lt => *max = (*max).min(n),
                            BinaryOperator::LtEq => *max = (*max).min(n),
                            BinaryOperator::Eq => {
                                *min = (*min).max(n);
                                *max = (*max).min(n);
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(sel) = selection {
        visit(sel, &is_ts, &mut min, &mut max);
    }
    // Guard against an inverted range, which would silently return nothing.
    if min > max {
        return (0, 0);
    }
    let _ = meta;
    (min, max)
}

/// Parse an interval such as `10s`, `1m`, `1h30m`, or a bare second count.
pub fn parse_interval(text: &str) -> anyhow::Result<i64> {
    let text = text.trim();
    if text.is_empty() {
        anyhow::bail!("empty time interval");
    }
    if let Ok(plain) = text.parse::<i64>() {
        anyhow::bail!("time interval '{}' needs a unit, e.g. {}s", text, plain);
    }
    let bytes = text.as_bytes();
    let mut total: i64 = 0;
    let mut i = 0usize;
    let mut saw_unit = false;
    while i < bytes.len() {
        let start = i;
        while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'-') {
            i += 1;
        }
        if start == i {
            anyhow::bail!("time interval '{}' has a value with no number", text);
        }
        let n: i64 = text[start..i]
            .parse()
            .map_err(|_| anyhow::anyhow!("bad number in time interval '{}'", text))?;
        let unit_start = i;
        while i < bytes.len() && !bytes[i].is_ascii_digit() {
            i += 1;
        }
        let unit = &text[unit_start..i];
        let mult = match unit {
            "ns" | "nanos" | "nanosecond" | "nanoseconds" => 1,
            "us" | "µs" | "micros" | "microsecond" | "microseconds" => 1_000,
            "ms" | "millis" | "millisecond" | "milliseconds" => 1_000_000,
            "s" | "sec" | "secs" | "second" | "seconds" => 1_000_000_000,
            "m" | "min" | "mins" | "minute" | "minutes" => 60_000_000_000,
            "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000_000_000,
            "d" | "day" | "days" => 86_400_000_000_000,
            "w" | "week" | "weeks" => 604_800_000_000_000,
            "" => anyhow::bail!("time interval '{}' is missing a unit", text),
            other => anyhow::bail!("unknown time interval unit '{}'", other),
        };
        saw_unit = true;
        total = total
            .checked_add(n.checked_mul(mult).ok_or_else(|| {
                anyhow::anyhow!("time interval '{}' overflows", text)
            })?)
            .ok_or_else(|| anyhow::anyhow!("time interval '{}' overflows", text))?;
    }
    if !saw_unit || total <= 0 {
        anyhow::bail!("time interval '{}' must be positive", text);
    }
    Ok(total)
}

fn build_group_by(
    select: &sqlparser::ast::Select,
    meta: &TableMeta,
) -> anyhow::Result<Vec<GroupTerm>> {
    let exprs = match &select.group_by {
        GroupByExpr::Expressions(e, _) => e,
        GroupByExpr::All(_) => anyhow::bail!("GROUP BY ALL is not supported"),
    };
    let mut terms = Vec::with_capacity(exprs.len());
    for e in exprs {
        match e {
            Expr::Function(f) if is_time_call(e) => {
                let (interval, offset) = time_call_args(f)?;
                // `time(1m)` is written in wall-clock units, but the raw
                // timestamps in a segment are in the table's own unit. Bucketing
                // 60_000_000_000 against a millisecond timestamp would produce
                // one bucket for the entire range instead of one per minute.
                let interval = duration_to_units(interval, meta.ts_unit)?;
                let offset = duration_to_units(offset, meta.ts_unit)?;
                terms.push(GroupTerm::TimeBucket { interval, offset });
            }
            Expr::Identifier(ident) => {
                let (index, _) = resolve(meta, &ident.value)?;
                terms.push(GroupTerm::Column { index });
            }
            other => anyhow::bail!(
                "unsupported GROUP BY expression {}; use a column or time(<interval>)",
                other
            ),
        }
    }
    Ok(terms)
}

fn time_call_args(f: &sqlparser::ast::Function) -> anyhow::Result<(i64, i64)> {
    let args = match &f.args {
        FunctionArguments::List(list) => &list.args,
        _ => anyhow::bail!("time() needs an interval, e.g. time(1m)"),
    };
    let mut interval = None;
    let mut offset = 0i64;
    for (i, arg) in args.iter().enumerate() {
        let expr = match arg {
            sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
            _ => anyhow::bail!("time() takes a literal interval"),
        };
        match expr {
            Expr::Value(SqlValue::Number(n, _)) if i == 0 => {
                interval = Some(parse_interval(n)?);
            }
            Expr::Value(SqlValue::Number(n, _)) if i == 1 => {
                offset = parse_interval(n).unwrap_or(0);
            }
            Expr::Value(SqlValue::SingleQuotedString(s)) if i == 0 => {
                interval = Some(parse_interval(s)?);
            }
            Expr::Value(SqlValue::SingleQuotedString(s)) if i == 1 => {
                offset = parse_interval(s).unwrap_or(0);
            }
            other => anyhow::bail!("time() takes literal arguments, got {}", other),
        }
    }
    let interval = interval.ok_or_else(|| anyhow::anyhow!("time() needs an interval"))?;
    Ok((interval, offset))
}

/// Convert a nanosecond duration into the raw integer units of a table's
/// timestamp column, rounding down so a bucket never spans a unit boundary.
fn duration_to_units(ns: i64, unit: tmdb_timestore::TsUnit) -> anyhow::Result<i64> {
    let per_unit: i64 = match unit {
        tmdb_timestore::TsUnit::Nanos => 1,
        tmdb_timestore::TsUnit::Micros => 1_000,
        tmdb_timestore::TsUnit::Millis => 1_000_000,
        tmdb_timestore::TsUnit::Seconds => 1_000_000_000,
    };
    Ok(ns / per_unit)
}

fn resolve<'a>(meta: &'a TableMeta, name: &str) -> anyhow::Result<(usize, &'a ColumnDef)> {
    meta.columns
        .iter()
        .enumerate()
        .find(|(_, c)| c.name.eq_ignore_ascii_case(name))
        .map(|(i, c)| (i, c))
        .ok_or_else(|| anyhow::anyhow!("unknown column '{}' in table '{}'", name, meta.name))
}

fn build_output(
    select: &sqlparser::ast::Select,
    meta: &TableMeta,
) -> anyhow::Result<ResolvedSelect> {
    let mut aggregates: Vec<Aggregate> = Vec::new();
    let mut out: Vec<OutputColumn> = Vec::new();
    let mut group_names: Vec<String> = Vec::new();

    // Seed the output with the GROUP BY columns so they lead the result, which
    // is the order every time-series client expects.
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for e in exprs {
            match e {
                Expr::Function(f) if is_time_call(e) => {
                    group_names.push("time".to_string());
                    out.push(OutputColumn {
                        name: "time".to_string(),
                        data_type: tmdb_timestore::DataType::Timestamp,
                        expr: None,
                        from_group: Some(group_names.len() - 1),
                        aggregate: None,
                    });
                }
                Expr::Identifier(ident) => {
                    let (_, col) = resolve(meta, &ident.value)?;
                    group_names.push(col.name.clone());
                    out.push(OutputColumn {
                        name: col.name.clone(),
                        data_type: col.data_type,
                        expr: None,
                        from_group: Some(group_names.len() - 1),
                        aggregate: None,
                    });
                }
                _ => {}
            }
        }
    }

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                if !out.is_empty() {
                    anyhow::bail!("SELECT * cannot be combined with GROUP BY");
                }
                for col in &meta.columns {
                    out.push(OutputColumn {
                        name: col.name.clone(),
                        data_type: col.data_type,
                        expr: Some(Expr::Identifier(sqlparser::ast::Ident {
                            value: col.name.clone(),
                            quote_style: None,
                        })),
                        from_group: None,
                        aggregate: None,
                    });
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                let name = default_output_name(expr);
                push_expr(&mut out, &mut aggregates, expr, name, meta)?;
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                push_expr(&mut out, &mut aggregates, expr, alias.value.clone(), meta)?;
            }
        }
    }

    if out.is_empty() {
        anyhow::bail!("SELECT must name at least one column");
    }
    Ok(ResolvedSelect {
        columns: out,
        aggregates,
    })
}

/// True for a `time(...)` bucket call.
fn is_time_call(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Function(f) if f.name.0.last().map(|p| p.value.to_ascii_lowercase()) == Some("time".to_string())
    )
}

fn push_expr(
    out: &mut Vec<OutputColumn>,
    aggregates: &mut Vec<Aggregate>,
    expr: &Expr,
    name: String,
    meta: &TableMeta,
) -> anyhow::Result<()> {
    // `SELECT time(1m), sum(v) ... GROUP BY time(1m)` names the group column
    // twice. The seeded group column already leads the output, so the second
    // mention would otherwise be treated as an ungrouped column.
    if is_time_call(expr) && out.iter().any(|c| c.from_group.is_some()) {
        return Ok(());
    }
    if let Some(agg) = as_aggregate(expr, meta)? {
        let index = aggregates.len();
        aggregates.push(Aggregate {
            func: agg.0,
            arg: agg.1,
        });
        out.push(OutputColumn {
            name,
            data_type: agg.2,
            expr: None,
            from_group: None,
            aggregate: Some(index),
        });
        return Ok(());
    }
    // A bare column in a grouped query still has to be a GROUP BY term.
    let data_type = match expr {
        Expr::Identifier(ident) => resolve(meta, &ident.value)?.1.data_type,
        _ => tmdb_timestore::DataType::Float64,
    };
    // The GROUP BY columns were already seeded at the head of the output, so
    // `SELECT host, count(*)` must not emit `host` a second time.
    if let Expr::Identifier(ident) = expr {
        let declared = meta
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&ident.value))
            .map(|c| c.name.clone())
            .unwrap_or_else(|| ident.value.clone());
        if out.iter().any(|c| c.name == declared) {
            return Ok(());
        }
    }
    out.push(OutputColumn {
        name,
        data_type,
        expr: Some(expr.clone()),
        from_group: None,
        aggregate: None,
    });
    Ok(())
}

/// Recognise an aggregate call, returning its function, argument and result type.
fn as_aggregate(
    expr: &Expr,
    meta: &TableMeta,
) -> anyhow::Result<Option<(AggFunc, Option<Expr>, tmdb_timestore::DataType)>> {
    let f = match expr {
        Expr::Function(f) => f,
        _ => return Ok(None),
    };
    let fname = match f.name.0.last().map(|p| p.value.to_ascii_lowercase()) {
        Some(n) => n,
        None => return Ok(None),
    };
    let func = match AggFunc::parse(&fname) {
        Some(f) => f,
        None => return Ok(None),
    };

    let args = match &f.args {
        FunctionArguments::List(list) => &list.args,
        _ => anyhow::bail!("{}() needs an argument or *", fname),
    };
    if args.is_empty() {
        anyhow::bail!("{}() needs an argument or *", fname);
    }
    if args.len() > 1 {
        anyhow::bail!("{}() takes one argument", fname);
    }
    let arg_expr = match &args[0] {
        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
        _ => anyhow::bail!("{}() takes a plain column or *", fname),
    };

    if arg_expr.is_none() && func != AggFunc::Count {
        anyhow::bail!("{}(*) is not supported; only count(*) takes a star", fname);
    }

    let output_type = match func {
        // COUNT is always an integer, matching SQL.
        AggFunc::Count => tmdb_timestore::DataType::Int64,
        // SUM and AVG widen to a real so a long series does not overflow.
        AggFunc::Sum | AggFunc::Avg => tmdb_timestore::DataType::Float64,
        // MIN, MAX, FIRST and LAST hand back the argument untouched, so
        // MIN(host) on a string column stays a string.
        _ => match &arg_expr {
            Some(Expr::Identifier(ident)) => resolve(meta, &ident.value)?.1.data_type,
            _ => tmdb_timestore::DataType::Float64,
        },
    };
    Ok(Some((func, arg_expr, output_type)))
}

fn default_output_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(i) => i.value.clone(),
        other => other.to_string(),
    }
}

fn sort_rows(
    rows: &mut [Vec<Option<Vec<u8>>>],
    columns: &[OutputColumn],
    order_by: &[sqlparser::ast::OrderByExpr],
) -> anyhow::Result<()> {
    // Resolve each ORDER BY item to an output column index first, so the sort
    // does not have to re-evaluate expressions against rows it no longer has.
    let mut keys: Vec<(usize, bool)> = Vec::with_capacity(order_by.len());
    for ob in order_by {
        let asc = !matches!(ob.asc, Some(false));
        let idx = match &ob.expr {
            Expr::Identifier(ident) => columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "ORDER BY '{}' is not in the SELECT list",
                        ident.value
                    )
                })?,
            // `ORDER BY 2` addresses the second output column.
            Expr::Value(SqlValue::Number(n, _)) => {
                let n: usize = n
                    .parse()
                    .map_err(|_| anyhow::anyhow!("ORDER BY position must be a number"))?;
                if n == 0 || n > columns.len() {
                    anyhow::bail!("ORDER BY position {} is out of range", n);
                }
                n - 1
            }
            Expr::Value(SqlValue::SingleQuotedString(s)) => columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(s))
                .ok_or_else(|| anyhow::anyhow!("ORDER BY '{}' is not in the SELECT list", s))?,
            other => anyhow::bail!("unsupported ORDER BY expression {}", other),
        };
        keys.push((idx, asc));
    }

    rows.sort_by(|a, b| {
        for (idx, asc) in &keys {
            let col = &columns[*idx];
            let av = Value::decode(&col.data_type, a[*idx].as_deref());
            let bv = Value::decode(&col.data_type, b[*idx].as_deref());
            let ord = match (av.sort_key(), bv.sort_key()) {
                ((ak, af, asx), (bk, bf, bsx)) => ak
                    .cmp(&bk)
                    .then_with(|| af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal))
                    .then_with(|| asx.cmp(&bsx)),
            };
            if ord != std::cmp::Ordering::Equal {
                return if *asc { ord } else { ord.reverse() };
            }
        }
std::cmp::Ordering::Equal
    });
    Ok(())
}

/// Reject an `HAVING` clause that cannot be evaluated.
///
/// Validation runs before the scan rather than per row, because an empty
/// table never reaches the evaluator and a query that names a column which
/// does not exist must fail even when there is nothing to filter. Letting it
/// return zero rows makes a typo indistinguishable from a genuine empty
/// result.
fn validate_having(
    having: &Expr,
    columns: &[OutputColumn],
    agg_map: &std::collections::HashMap<String, (usize, DataType)>,
    meta: &TableMeta,
) -> anyhow::Result<()> {
    match having {
        Expr::Identifier(ident) => {
            if columns.iter().any(|c| c.name.eq_ignore_ascii_case(&ident.value)) {
                return Ok(());
            }
            // An aggregate may be addressed by its canonical signature only
            // through a function call, so a bare name must be a real column.
            anyhow::bail!("unknown column '{}' in HAVING", ident.value)
        }
        Expr::CompoundIdentifier(parts) => {
            let name = parts.last().map(|p| p.value.as_str()).unwrap_or_default();
            if columns.iter().any(|c| c.name.eq_ignore_ascii_case(name)) {
                return Ok(());
            }
            anyhow::bail!("unknown column '{}' in HAVING", name)
        }
        Expr::Function(_) => {
            // `meta` supplies the argument's type, so a call that is not an
            // aggregate at all is reported here rather than at execution.
            let Some((func, arg, _)) = as_aggregate(having, meta)? else {
                anyhow::bail!("'{}' is not an aggregate function", having);
            };
            let key = aggregate_key(&func, &arg);
            if !agg_map.contains_key(&key) {
                anyhow::bail!("aggregate '{}' is not available after aggregation", having);
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_having(left, columns, agg_map, meta)?;
            validate_having(right, columns, agg_map, meta)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => validate_having(expr, columns, agg_map, meta),
        Expr::Between { expr, low, high, .. } => {
            validate_having(expr, columns, agg_map, meta)?;
            validate_having(low, columns, agg_map, meta)?;
            validate_having(high, columns, agg_map, meta)
        }
        Expr::InList { expr, list, .. } => {
            validate_having(expr, columns, agg_map, meta)?;
            for item in list {
                validate_having(item, columns, agg_map, meta)?;
            }
            Ok(())
        }
        Expr::Like { expr, pattern, .. } => {
            validate_having(expr, columns, agg_map, meta)?;
            validate_having(pattern, columns, agg_map, meta)
        }
        Expr::Value(_) => Ok(()),
        other => anyhow::bail!("unsupported expression in HAVING: {}", other),
    }
}

/// Collect every aggregate call in `expr`, keeping first-seen order and
/// dropping duplicates by canonical signature.
///
/// `HAVING count(*) > 5` names an aggregate the SELECT list may not project,
/// so those have to be added to the aggregate plan and then thrown away.
fn collect_having_aggregates(
    expr: &Expr,
    meta: &TableMeta,
    out: &mut Vec<Aggregate>,
) -> anyhow::Result<()> {
    match expr {
        Expr::Function(_) => {
            if let Some((func, arg, _)) = as_aggregate(expr, meta)? {
                let key = aggregate_key(&func, &arg);
                if !out.iter().any(|a| aggregate_key(&a.func, &a.arg) == key) {
                    out.push(Aggregate { func, arg });
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_having_aggregates(left, meta, out)?;
            collect_having_aggregates(right, meta, out)?;
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => collect_having_aggregates(expr, meta, out)?,
        Expr::Between { expr, low, high, .. } => {
            collect_having_aggregates(expr, meta, out)?;
            collect_having_aggregates(low, meta, out)?;
            collect_having_aggregates(high, meta, out)?;
        }
        Expr::InList { expr, list, .. } => {
            collect_having_aggregates(expr, meta, out)?;
            for item in list {
                collect_having_aggregates(item, meta, out)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            collect_having_aggregates(expr, meta, out)?;
            collect_having_aggregates(pattern, meta, out)?;
        }
        _ => {}
    }
    Ok(())
}

/// Apply `HAVING`, keeping only rows whose predicate is TRUE.
///
/// SQL keeps a row only when the predicate is TRUE: NULL and FALSE both drop
/// it. This runs after aggregation, so it sees GROUP BY columns and aggregates
/// but never raw rows — which is the whole point of the clause.
fn filter_having(
    rows: Vec<Vec<Option<Vec<u8>>>>,
    having: &Expr,
    columns: &[OutputColumn],
    meta: &TableMeta,
    agg_map: &std::collections::HashMap<String, (usize, DataType)>,
    ts: i64,
) -> anyhow::Result<Vec<Vec<Option<Vec<u8>>>>> {
    let mut kept = Vec::with_capacity(rows.len());
    for values in rows {
        let row = RowCtx {
            values: &values,
            meta,
            output: Some(columns),
            aggregates: Some(agg_map),
        };
        if eval_expr(having, &row, ts)? == Value::Bool(true) {
            kept.push(values);
        }
    }
    Ok(kept)
}

/// `SELECT DISTINCT`: drop duplicate output rows, keeping the first.
///
/// Runs on the projected row, so two input rows that differ only in a column
/// that was not selected really are duplicates — that is what DISTINCT asks
/// for.
fn distinct_rows(rows: Vec<Vec<Option<Vec<u8>>>>) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut seen = std::collections::HashSet::with_capacity(rows.len());
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if seen.insert(row.clone()) {
            out.push(row);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intervals_parse_with_units() {
        assert_eq!(parse_interval("1s").unwrap(), 1_000_000_000);
        assert_eq!(parse_interval("10ms").unwrap(), 10_000_000);
        assert_eq!(parse_interval("1m").unwrap(), 60_000_000_000);
        assert_eq!(parse_interval("1h").unwrap(), 3_600_000_000_000);
        assert_eq!(parse_interval("1d").unwrap(), 86_400_000_000_000);
        assert_eq!(parse_interval("1h30m").unwrap(), 5_400_000_000_000);
    }

    #[test]
    fn a_unitless_interval_is_refused() {
        // `GROUP BY time(5)` silently meaning 5 nanoseconds helped nobody.
        let err = parse_interval("5").unwrap_err();
        assert!(err.to_string().contains("needs a unit"), "{}", err);
        assert!(parse_interval("0s").is_err());
        assert!(parse_interval("5x").is_err());
        assert!(parse_interval("").is_err());
    }

    #[test]
    fn buckets_align_for_negative_timestamps() {
        let interval = 60_000_000_000i64;
        assert_eq!(bucket_start(0, interval, 0), 0);
        assert_eq!(bucket_start(interval - 1, interval, 0), 0);
        assert_eq!(bucket_start(interval, interval, 0), interval);
        assert_eq!(bucket_start(-1, interval, 0), -interval);
        assert_eq!(bucket_start(-interval - 1, interval, 0), -2 * interval);
    }

    #[test]
    fn like_matches_sql_wildcards() {
        assert!(like_matches("hello", "h%"));
        assert!(like_matches("hello", "%llo"));
        assert!(like_matches("hello", "h_llo"));
        assert!(like_matches("hello", "h%o"));
        assert!(!like_matches("hello", "h_lo"));
        assert!(like_matches("", "%"));
        assert!(like_matches("aaa", "a%%a"));
    }
}
