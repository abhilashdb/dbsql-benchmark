# SQL Warehouse Load / Benchmark Tool

Ramps up concurrent query load against a Databricks SQL warehouse in steps, capturing per-query
latency, Query Result Cache (QRC) hit rate, and queueing metrics into a Delta table. Includes an
AI/BI dashboard for comparing two benchmark runs side by side (e.g. warehouse size A vs B, QRC
enabled vs disabled).

## Project structure

- `sql_benchmark_driver` - the main driver notebook. Run its cells top to bottom.
- `queries/1.sql` ... `queries/5.sql` - the benchmark query set, run against the public
  `samples.tpch` sample data (point filter, aggregation, joins, a heavier multi-way join, and an
  unbounded window function). Swap these out for your own `.sql` files to benchmark different
  workloads - files are loaded and ordered numerically by filename prefix.
- `dashboard/` (published separately) - "SQL Warehouse Benchmark Results", a paired run-comparison
  dashboard (Run A vs Run B) with latency percentiles, throughput, QRC hit rate, and queue wait.

## Setup

Before running the driver notebook, set these widget parameters:

| Parameter | Required | Description |
| --- | --- | --- |
| `warehouse_id` | yes | ID of the SQL warehouse to load-test |
| `queries_folder` | yes | Workspace path to a folder of `.sql` files (defaults to `queries/` alongside this notebook) |
| `results_table` | yes | `catalog.schema.table` to append results to (created on first write) |
| `run_name` | no | Human-friendly label for this run, shown in the dashboard's run picker |
| `initial_concurrency` / `max_concurrency` / `step_size` | no | Ramp shape: starting workers, ceiling, and how many workers to add per step |
| `step_up_duration_sec` | no | How long to hold before adding the next step of concurrency |
| `sustain_duration_sec` | no | How long to sustain at `max_concurrency` before stopping |
| `statement_timeout_sec` | no | Per-query statement timeout |
| `disable_qrc` | no | Set `true` to disable the Query Result Cache for this run (for an apples-to-apples cold comparison) |

You'll need `CREATE TABLE` permission on the schema behind `results_table`, and `CAN_USE`
permission on the target warehouse.

## How it works

1. **Parameters** - widgets described above.
2. **Install Python SQL Connector** - installs `databricks-sql-connector` and restarts Python.
   The connector opens a real, stateful session per connection, which is what lets
   `session_configuration` (e.g. `use_cached_result`) actually persist across statements - unlike
   the stateless Statement Execution API.
3. **Setup** - loads widgets, loads and hashes the `.sql` files, and looks up warehouse metadata.
4. **Worker** - each worker thread opens one persistent connection and loops running queries
   round-robin, recording latency/status/rows/statement_id per query.
5. **Ramp controller** - starts at `initial_concurrency`, adds `step_size` workers every
   `step_up_duration_sec` until `max_concurrency`, sustains for `sustain_duration_sec`, then stops.
6. **Execute** - runs the ramp and blocks until the run completes.
7. **Persist results** - writes all captured rows to `results_table`.
8. **Quick summary** - p50/p90/p99 latency and throughput for the run just completed.
9. **QRC hit rate** - enriches the run's rows with Query History API metrics (cache hit, queue
   wait, server-side total time) via a rate-limited client, then prints a summary.

## Known caveat: query 5 (unbounded window function)

`queries/5.sql` has no `LIMIT` - it ranks orders per customer and can return a very large result
set. The Python SQL Connector's `cursor.execute()` blocks until it has fully resolved the result
before `fetchmany()` can slice it, so this query's *client-observed* latency can be dominated by
result materialization/transfer time under concurrency, even though the warehouse itself finishes
quickly (verified via Query History: `server_total_time_ms` stays low while client latency spikes).
This is inherent to the connector's handling of large, unbounded results - it reproduces
identically whether using the default Thrift path or `use_kernel=True` (Statement Execution API).
If you want apples-to-apples client latency across all queries, cap result size at the SQL level
(e.g. wrap with an outer `LIMIT`) - just be aware that adds a non-Photon `CollectLimit` step, so
it's a tradeoff, not a free fix.
