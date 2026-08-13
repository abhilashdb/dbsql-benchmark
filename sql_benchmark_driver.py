# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Parameters
# SQL Warehouse Load / Benchmark Driver
# Ramps up concurrent query load against a SQL warehouse in steps, capturing per-query latency into a Delta table.

dbutils.widgets.text("warehouse_id", "", "Warehouse ID (required - set to your SQL warehouse's ID)")
dbutils.widgets.text("queries_folder", "/Workspace/Users/<your_email>/sql_benchmark/queries", "Queries folder (workspace path)")
dbutils.widgets.text("results_table", "main.default.sql_benchmark_results", "Results Delta table (catalog.schema.table)")
dbutils.widgets.text("run_name", "", "Run name (optional)")
dbutils.widgets.text("initial_concurrency", "1", "Initial concurrency")
dbutils.widgets.text("max_concurrency", "8", "Max concurrency")
dbutils.widgets.text("step_size", "1", "Concurrency step size")
dbutils.widgets.text("step_up_duration_sec", "30", "Step-up duration (sec) - time to hold before adding more concurrency")
dbutils.widgets.text("sustain_duration_sec", "30", "Sustain duration at max concurrency (sec)")
dbutils.widgets.text("statement_timeout_sec", "120", "Per-query statement timeout (sec)")


# COMMAND ----------

# DBTITLE 1,Install Python SQL Connector (requires a kernel restart)
# Switching the worker from the stateless Statement Execution API SDK calls to the Python SQL
# Connector (databricks-sql-connector). The connector opens a real, stateful session per
# connection, so session_configuration (e.g. use_cached_result=false to disable QRC) actually
# persists across statements - unlike w.statement_execution.execute_statement(), which is a fresh
# stateless call every time and cannot honor a SET issued on a prior call.
# NOTE: dbutils.widgets values (set in the Parameters cell) survive a Python restart; plain
# variables do not, so every downstream cell must be re-run after this one.
%pip install -q databricks-sql-connector
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Setup: load parameters, queries and warehouse metadata
import os, re, time, threading, hashlib
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
import databricks.sql as dbsql

w = WorkspaceClient()

warehouse_id = dbutils.widgets.get("warehouse_id")
queries_folder = dbutils.widgets.get("queries_folder")
results_table = dbutils.widgets.get("results_table")
run_name = dbutils.widgets.get("run_name") or None
initial_concurrency = int(dbutils.widgets.get("initial_concurrency"))
max_concurrency = int(dbutils.widgets.get("max_concurrency"))
step_size = int(dbutils.widgets.get("step_size"))
step_up_duration_sec = int(dbutils.widgets.get("step_up_duration_sec"))
sustain_duration_sec = int(dbutils.widgets.get("sustain_duration_sec"))
statement_timeout_sec = int(dbutils.widgets.get("statement_timeout_sec"))
disable_qrc = dbutils.widgets.get("disable_qrc") == "true"

# --- Safety net: hardcoded hard stop, not user-configurable via a widget ---
# The ramp controller never overshoots max_concurrency (each step is clamped to it), so checking
# the "Max concurrency" widget itself is sufficient to bound the whole run.
MAX_ALLOWED_CONCURRENCY = 500
if max_concurrency > MAX_ALLOWED_CONCURRENCY:
    raise ValueError(
        f"Refusing to start run: max_concurrency={max_concurrency} exceeds the hardcoded safety cap of "
        f"{MAX_ALLOWED_CONCURRENCY} concurrent queries. Lower the 'Max concurrency' widget to "
        f"{MAX_ALLOWED_CONCURRENCY} or below to proceed. (This cap is hardcoded in the notebook code; "
        f"edit MAX_ALLOWED_CONCURRENCY in this cell if you deliberately need to exceed it.)"
    )
print(f"Safety check passed: max_concurrency {max_concurrency} <= hardcoded cap {MAX_ALLOWED_CONCURRENCY}")

# Load .sql files from the queries folder, sorted numerically (1.sql, 2.sql, ...)
# query_hash = SHA-256 of the exact file content, so the same query text can be matched across runs
# even if the file gets renamed/reordered later (query_file/query_number are not stable identifiers).
def load_queries(folder):
    files = [f for f in os.listdir(folder) if f.endswith(".sql")]
    def sort_key(fname):
        m = re.match(r"(\d+)", fname)
        return int(m.group(1)) if m else 0
    files.sort(key=sort_key)
    qs = []
    for f in files:
        with open(os.path.join(folder, f)) as fh:
            sql = fh.read()
        query_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        qs.append({"query_file": f, "query_number": sort_key(f), "sql": sql, "query_hash": query_hash})
    return qs

queries = load_queries(queries_folder)
assert len(queries) > 0, f"No .sql files found in {queries_folder}"
print(f"Loaded {len(queries)} queries: {[q['query_file'] for q in queries]}")

# Warehouse metadata (used for tagging + dashboard filters), looked up from the user-supplied warehouse_id
wh = w.warehouses.get(warehouse_id)
warehouse_name = wh.name
warehouse_size = f"{wh.cluster_size}" + (" Serverless" if wh.enable_serverless_compute else "")
warehouse_type = wh.warehouse_type.value if wh.warehouse_type else None
warehouse_min_clusters = wh.min_num_clusters
warehouse_max_clusters = wh.max_num_clusters
warehouse_num_clusters = wh.num_clusters
warehouse_auto_stop_mins = wh.auto_stop_mins

print(f"Target warehouse: {warehouse_name} ({warehouse_size}, id={warehouse_id}, type={warehouse_type})")
print(
    f"Auto-scaling config: min_clusters={warehouse_min_clusters}, max_clusters={warehouse_max_clusters}, "
    f"current_clusters={warehouse_num_clusters}, serverless={wh.enable_serverless_compute}, "
    f"auto_stop_mins={warehouse_auto_stop_mins}"
)

# Connection details for the Python SQL Connector (used by the Worker cell to open one persistent,
# session-backed connection per worker thread).
server_hostname = wh.odbc_params.hostname
http_path = wh.odbc_params.path
print(f"QRC mode: {'DISABLED (use_cached_result=false)' if disable_qrc else 'enabled (default)'}")

# Workspace ID: stored on every row so it can be used (together with statement_id) as an efficient
# join key against system.query.history, which is also partitioned/keyed by workspace_id.
workspace_id = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().get())
print(f"Workspace ID: {workspace_id}")


# COMMAND ----------

# DBTITLE 1,Worker: execute one query and record its latency
results_lock = threading.Lock()
results = []
stop_event = threading.Event()
current_concurrency = {"value": 0}  # updated by the ramp controller; read by workers when they start a query

def make_connection():
    """Open one persistent, session-backed connection. session_configuration is applied once here
    and then genuinely persists for every statement executed through this connection's cursors -
    this is what lets us reliably disable QRC (use_cached_result=false), unlike the previous
    w.statement_execution.execute_statement() calls, which are stateless per call with no session_id
    and could not honor a SET issued on a prior call (verified empirically)."""
    return dbsql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: w.config.authenticate,  # reuses the notebook's own auth, no literal token
        session_configuration={
            "use_cached_result": not disable_qrc,
            "statement_timeout": statement_timeout_sec,
        },
    )

def run_one_query(worker_id, query, target_concurrency, run_id, cursor):
    start_ts = datetime.now(timezone.utc)
    status, error_message, statement_id, rows_returned = "SUCCESS", None, None, None
    try:
        cursor.execute(query["sql"])
        statement_id = getattr(cursor, "query_id", None)
        if cursor.description is not None:
            # Bound how much we pull to the client (mirrors the previous row_limit=10000 cap on the
            # SDK call); the warehouse still computes the full result server-side either way.
            rows = cursor.fetchmany(10000)
            rows_returned = cursor.rowcount if (cursor.rowcount is not None and cursor.rowcount >= 0) else len(rows)
        else:
            rows_returned = cursor.rowcount if (cursor.rowcount is not None and cursor.rowcount >= 0) else 0
    except Exception as e:
        status = "FAILED"
        error_message = str(e)[:2000]
        statement_id = statement_id or getattr(cursor, "query_id", None)
    end_ts = datetime.now(timezone.utc)
    latency_ms = (end_ts - start_ts).total_seconds() * 1000
    with results_lock:
        results.append({
            "run_id": run_id, "run_start_ts": run_start_ts, "run_name": run_name,
            "workspace_id": workspace_id,
            "warehouse_id": warehouse_id, "warehouse_name": warehouse_name, "warehouse_size": warehouse_size,
            "warehouse_min_clusters": warehouse_min_clusters, "warehouse_max_clusters": warehouse_max_clusters,
            "warehouse_num_clusters": warehouse_num_clusters, "warehouse_type": warehouse_type,
            "warehouse_auto_stop_mins": warehouse_auto_stop_mins,
            "query_file": query["query_file"], "query_number": query["query_number"], "query_hash": query["query_hash"],
            "target_concurrency": target_concurrency, "worker_id": worker_id,
            "start_ts": start_ts, "end_ts": end_ts, "latency_ms": latency_ms,
            "status": status, "error_message": error_message,
            "statement_id": statement_id, "rows_returned": rows_returned,
            "qrc_disabled_mode": disable_qrc,
        })

def worker_loop(worker_id, run_id):
    i = worker_id
    n = len(queries)
    conn = make_connection()
    cursor = conn.cursor()
    try:
        while not stop_event.is_set():
            query = queries[i % n]
            i += 1
            try:
                run_one_query(worker_id, query, current_concurrency["value"], run_id, cursor)
            except Exception:
                # Cursor/connection may be unusable after an unexpected failure (e.g. a dropped
                # session) - reopen so this worker can keep going for the rest of the run.
                try:
                    cursor.close()
                    conn.close()
                except Exception:
                    pass
                conn = make_connection()
                cursor = conn.cursor()
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


# COMMAND ----------

# DBTITLE 1,Ramp controller: step up concurrency, sustain, then stop
def run_benchmark():
    global run_start_ts, run_id
    run_start_ts = datetime.now(timezone.utc)
    # Run start timestamp (second precision) doubles as a unique run identifier
    run_id = run_start_ts.strftime("%Y%m%d_%H%M%S")
    print(f"Starting run_id={run_id} run_name={run_name!r} warehouse={warehouse_name} ({warehouse_size})")

    stop_event.clear()
    results.clear()
    threads = []

    def start_workers(n):
        start_idx = len(threads)
        for wid in range(start_idx, start_idx + n):
            t = threading.Thread(target=worker_loop, args=(wid, run_id), daemon=True)
            t.start()
            threads.append(t)

    current_concurrency["value"] = initial_concurrency
    start_workers(initial_concurrency)
    print(f"[t=0s] concurrency -> {initial_concurrency}")

    while current_concurrency["value"] < max_concurrency:
        time.sleep(step_up_duration_sec)
        add_n = min(step_size, max_concurrency - current_concurrency["value"])
        start_workers(add_n)
        current_concurrency["value"] += add_n
        elapsed = (datetime.now(timezone.utc) - run_start_ts).total_seconds()
        print(f"[t={elapsed:.0f}s] concurrency -> {current_concurrency['value']}")

    print(f"Sustaining at {current_concurrency['value']} concurrent workers for {sustain_duration_sec}s")
    time.sleep(sustain_duration_sec)

    stop_event.set()
    for t in threads:
        t.join(timeout=statement_timeout_sec + 5)

    run_end_ts = datetime.now(timezone.utc)
    print(f"Run complete. run_id={run_id}, total queries executed={len(results)}, wall time={(run_end_ts - run_start_ts).total_seconds():.1f}s")
    return run_id


# COMMAND ----------

# DBTITLE 1,Execute the benchmark run
run_id = run_benchmark()


# COMMAND ----------

# DBTITLE 1,Persist results to the Delta table
from pyspark.sql import types as T

schema = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("run_start_ts", T.TimestampType()),
    T.StructField("run_name", T.StringType()),
    T.StructField("workspace_id", T.StringType()),
    T.StructField("warehouse_id", T.StringType()),
    T.StructField("warehouse_name", T.StringType()),
    T.StructField("warehouse_size", T.StringType()),
    T.StructField("warehouse_min_clusters", T.IntegerType()),
    T.StructField("warehouse_max_clusters", T.IntegerType()),
    T.StructField("warehouse_num_clusters", T.IntegerType()),
    T.StructField("warehouse_type", T.StringType()),
    T.StructField("warehouse_auto_stop_mins", T.IntegerType()),
    T.StructField("query_file", T.StringType()),
    T.StructField("query_number", T.IntegerType()),
    T.StructField("query_hash", T.StringType()),
    T.StructField("target_concurrency", T.IntegerType()),
    T.StructField("worker_id", T.IntegerType()),
    T.StructField("start_ts", T.TimestampType()),
    T.StructField("end_ts", T.TimestampType()),
    T.StructField("latency_ms", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("error_message", T.StringType()),
    T.StructField("statement_id", T.StringType()),
    T.StructField("rows_returned", T.LongType()),
    T.StructField("qrc_disabled_mode", T.BooleanType()),
])

if results:
    df = spark.createDataFrame(results, schema=schema)
    df.write.mode("append").saveAsTable(results_table)
    print(f"Wrote {len(results)} rows to {results_table} for run_id={run_id}")
else:
    print("No results captured - nothing written.")


# COMMAND ----------

# DBTITLE 1,Quick summary of this run
summary_df = spark.sql(f"""
  SELECT
    run_id, run_name, warehouse_name, warehouse_size,
    COUNT(*) AS total_queries,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_queries,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
    MAX(target_concurrency) AS peak_concurrency,
    approx_percentile(latency_ms, 0.5) AS p50_latency_ms,
    approx_percentile(latency_ms, 0.9) AS p90_latency_ms,
    approx_percentile(latency_ms, 0.99) AS p99_latency_ms,
    COUNT(*) / GREATEST(unix_timestamp(MAX(end_ts)) - unix_timestamp(MIN(start_ts)), 1) AS throughput_qps
  FROM {results_table}
  WHERE run_id = '{run_id}'
  GROUP BY run_id, run_name, warehouse_name, warehouse_size
""")
display(summary_df)


# COMMAND ----------

# DBTITLE 1,Query Result Cache (QRC) hit rate for this run
# Uses the Query History API (w.query_history.list) instead of system.query.history: it returns
# per-statement metrics (is_final=True) almost immediately after a statement finishes, so we don't
# have to wait out the multi-minute system-table ingestion lag. Results are also written straight
# back into the results table (qrc_hit, queue_wait_ms, provisioning_wait_ms, server_total_time_ms),
# so downstream analysis / the dashboard never needs to join against system.query.history for this run.
from databricks.sdk.service.sql import QueryFilter
from collections import deque

# The Query History API is rate-limited to 10 requests/second. Track call timestamps in a sliding
# 1s window and throttle before every call so we never exceed that, regardless of how many batches
# a run needs (bigger runs -> more chunks -> more calls, all funneled through this one gate).
_QH_MAX_RPS = 10
_qh_call_times = deque()

def _rate_limited_query_history_list(**kwargs):
    now = time.time()
    while _qh_call_times and now - _qh_call_times[0] > 1.0:
        _qh_call_times.popleft()
    if len(_qh_call_times) >= _QH_MAX_RPS:
        sleep_for = 1.0 - (now - _qh_call_times[0]) + 0.02  # small buffer for clock/timing slop
        if sleep_for > 0:
            time.sleep(sleep_for)
    resp = w.query_history.list(**kwargs)
    _qh_call_times.append(time.time())
    return resp

def fetch_query_history_metrics(statement_ids, batch_size=100, max_wait_sec=30, poll_interval_sec=2):
    """Fetch Query History API records for statement_ids, briefly retrying any not yet finalized."""
    pending = set(statement_ids)
    records = {}
    deadline = time.time() + max_wait_sec
    while pending and time.time() < deadline:
        batch_ids = list(pending)
        for i in range(0, len(batch_ids), batch_size):
            chunk = batch_ids[i:i + batch_size]
            resp = _rate_limited_query_history_list(
                filter_by=QueryFilter(statement_ids=chunk), include_metrics=True, max_results=batch_size
            )
            for q in resp.res:
                if q.is_final:
                    records[q.query_id] = q
                    pending.discard(q.query_id)
        if pending:
            time.sleep(poll_interval_sec)
    return records, pending

def extract_fields(q):
    m = q.metrics
    comp_start = m.query_compilation_start_timestamp
    queue_wait_ms = (comp_start - m.overloading_queue_start_timestamp) if (m.overloading_queue_start_timestamp and comp_start) else 0.0
    provisioning_wait_ms = (comp_start - m.provisioning_queue_start_timestamp) if (m.provisioning_queue_start_timestamp and comp_start) else 0.0
    return {
        "statement_id": q.query_id,
        "qrc_hit": bool(m.result_from_cache),
        "queue_wait_ms": float(queue_wait_ms),
        "provisioning_wait_ms": float(provisioning_wait_ms),
        "server_total_time_ms": float(m.total_time_ms) if m.total_time_ms is not None else None,
    }

statement_ids_this_run = [r["statement_id"] for r in results if r.get("statement_id")]
qh_records, still_pending = fetch_query_history_metrics(statement_ids_this_run)
print(f"Fetched Query History API metrics for {len(qh_records)}/{len(statement_ids_this_run)} statements"
      + (f" ({len(still_pending)} not finalized after waiting - will show as NULL)" if still_pending else ""))

enrichment_rows = [extract_fields(q) for q in qh_records.values()]
if enrichment_rows:
    enrichment_schema = T.StructType([
        T.StructField("statement_id", T.StringType()),
        T.StructField("qrc_hit", T.BooleanType()),
        T.StructField("queue_wait_ms", T.DoubleType()),
        T.StructField("provisioning_wait_ms", T.DoubleType()),
        T.StructField("server_total_time_ms", T.DoubleType()),
    ])
    enrichment_df = spark.createDataFrame(enrichment_rows, schema=enrichment_schema)
    enrichment_df.createOrReplaceTempView("_qh_enrichment")
    spark.sql(f"""
      MERGE INTO {results_table} r
      USING _qh_enrichment e
      ON r.statement_id = e.statement_id AND r.run_id = '{run_id}'
      WHEN MATCHED THEN UPDATE SET
        r.qrc_hit = e.qrc_hit,
        r.queue_wait_ms = e.queue_wait_ms,
        r.provisioning_wait_ms = e.provisioning_wait_ms,
        r.server_total_time_ms = e.server_total_time_ms
    """)
    print(f"Logged Query History API metrics for {len(enrichment_rows)} rows of run_id={run_id}")

qrc_df = spark.sql(f"""
  SELECT
    run_id,
    COUNT(*) AS total_queries,
    SUM(CASE WHEN qrc_hit IS NULL THEN 1 ELSE 0 END) AS not_yet_enriched,
    100.0 * SUM(CASE WHEN qrc_hit THEN 1 ELSE 0 END) / COUNT(*) AS pct_qrc_hit,
    100.0 * SUM(CASE WHEN queue_wait_ms > 0 OR provisioning_wait_ms > 0 THEN 1 ELSE 0 END) / COUNT(*) AS pct_queued,
    AVG(queue_wait_ms) AS avg_queue_wait_ms,
    AVG(latency_ms) AS avg_client_latency_ms,
    AVG(server_total_time_ms) AS avg_server_total_time_ms,
    AVG(latency_ms) - AVG(server_total_time_ms) AS avg_client_minus_server_gap_ms
  FROM {results_table}
  WHERE run_id = '{run_id}'
  GROUP BY run_id
""")
display(qrc_df)
# avg_client_minus_server_gap_ms is the part of latency our benchmark measured that the warehouse
# itself doesn't account for (compilation+execution+queueing) - e.g. client thread/polling overhead
# under high concurrency, or admission delay before the warehouse starts compiling.