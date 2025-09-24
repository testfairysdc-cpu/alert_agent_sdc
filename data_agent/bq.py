from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from data_agent.config import BQ_PROJECT, BQ_LOCATION, BQ_DATASET, ALLOW_CROSS_DATASET
from data_agent.utils.log import log_step


def _ensure_select(sql: str) -> None:
    lowered = (sql or "").strip().lower()
    if not lowered.startswith("select"):
        raise ValueError("Only read-only SELECT queries are allowed.")


def _enforce_dataset(sql: str) -> None:
    if ALLOW_CROSS_DATASET:
        return
    # Simple guard: require dataset reference. Allow both table refs and INFORMATION_SCHEMA refs.
    lowered = (sql or "").lower()
    hint_table = f"`{BQ_PROJECT}.{BQ_DATASET}."
    hint_info_schema = f"`{BQ_PROJECT.lower()}.{BQ_DATASET.lower()}`.information_schema"
    if hint_table in sql:
        return
    if hint_info_schema in lowered:
        return
    # As a fallback, allow the variant with backtick before dot (e.g., `proj.ds`.information_schema)
    hint_backtick_then_dot = f"`{BQ_PROJECT}.{BQ_DATASET}`."
    if hint_backtick_then_dot in sql:
        return
    raise ValueError(
        f"Query must reference dataset `{BQ_PROJECT}.{BQ_DATASET}`."
    )


def run_query(
    sql: str,
    *,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    maximum_rows: int = 1000,
    dry_run: bool = False,
) -> Dict[str, Any]:
    _ensure_select(sql)
    log_step("Validating dataset scope and query safety…")
    _enforce_dataset(sql)

    client = bigquery.Client(project=project_id or BQ_PROJECT)
    job_config = bigquery.QueryJobConfig()

    if dry_run:
        job_config.dry_run = True
        job_config.use_query_cache = False

    if parameters:
        qp: List[bigquery.ScalarQueryParameter] = []
        for name, value in parameters.items():
            if isinstance(value, bool):
                bq_type = "BOOL"
            elif isinstance(value, int):
                bq_type = "INT64"
            elif isinstance(value, float):
                bq_type = "FLOAT64"
            else:
                bq_type = "STRING"
            qp.append(bigquery.ScalarQueryParameter(name, bq_type, value))
        job_config.query_parameters = qp

    try:
        log_step("Submitting BigQuery job…")
        job = client.query(sql, job_config=job_config, location=location or BQ_LOCATION)
        if dry_run:
            log_step("Dry run completed.")
            return {
                "status": "success",
                "dry_run": True,
                "total_bytes_processed": job.total_bytes_processed,
                "sql": sql,
            }
        log_step("Waiting for query results…")
        result = job.result()
        field_names = [f.name for f in result.schema]
        rows: List[Dict[str, Any]] = [
            {k: row[k] for k in field_names} for row in result
        ]
        if maximum_rows and maximum_rows > 0:
            rows = rows[:maximum_rows]
        schema = [
            {"name": f.name, "type": f.field_type, "mode": getattr(f, "mode", None)}
            for f in result.schema
        ]
        log_step("Query succeeded.")
        return {
            "status": "success",
            "rows": rows,
            "num_rows": len(rows),
            "schema": schema,
            "job_id": job.job_id,
            "sql": sql,
        }
    except Exception as exc:  # noqa: BLE001
        log_step(f"Query failed: {exc}")
        return {"status": "error", "error_message": str(exc), "sql": sql}


def get_schema_summary(max_tables: int = 25, max_columns_per_table: int = 30) -> str:
    # Build a compact schema context from INFORMATION_SCHEMA
    sql = f"""
    SELECT table_name, column_name, data_type
    FROM `{BQ_PROJECT}.{BQ_DATASET}`.INFORMATION_SCHEMA.COLUMNS
    ORDER BY table_name, ordinal_position
    """
    res = run_query(sql)
    if res.get("status") != "success":
        return ""
    rows = res.get("rows", [])
    # Aggregate by table
    table_to_cols: Dict[str, List[str]] = {}
    for r in rows:
        table_to_cols.setdefault(r["table_name"], []).append(
            f"{r['column_name']}:{r['data_type']}"
        )
    parts: List[str] = []
    for idx, (tbl, cols) in enumerate(table_to_cols.items()):
        if idx >= max_tables:
            parts.append("...")
            break
        if len(cols) > max_columns_per_table:
            cols = cols[:max_columns_per_table] + ["..."]
        parts.append(f"{tbl} -> " + ", ".join(cols))
    return "\n".join(parts)


def list_tables() -> Dict[str, Any]:
    sql = f"SELECT table_name FROM `{BQ_PROJECT}.{BQ_DATASET}`.INFORMATION_SCHEMA.TABLES ORDER BY table_name"
    return run_query(sql)


def count_tables() -> Dict[str, Any]:
    sql = f"SELECT COUNT(*) AS table_count FROM `{BQ_PROJECT}.{BQ_DATASET}`.INFORMATION_SCHEMA.TABLES"
    return run_query(sql)


def get_tables_and_columns(max_tables: int = 200, max_cols: int = 60) -> Dict[str, List[Dict[str, Any]]]:
    """Return mapping: table_name -> list of {column_name, data_type}.

    Limits are applied to keep prompt sizes manageable.
    """
    sql = f"""
    SELECT table_name, column_name, data_type, ordinal_position
    FROM `{BQ_PROJECT}.{BQ_DATASET}`.INFORMATION_SCHEMA.COLUMNS
    ORDER BY table_name, ordinal_position
    """
    res = run_query(sql)
    if res.get("status") != "success":
        return {}
    rows = res.get("rows", [])
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        t = r["table_name"]
        cols = out.setdefault(t, [])
        if len(cols) < max_cols:
            cols.append({"column_name": r["column_name"], "data_type": r["data_type"]})
        # enforce table limit
        if len(out) >= max_tables and t not in out:
            break
    return out


def table_row_counts() -> Dict[str, Any]:
    """Return row counts per table with fallbacks, and include debug attempts."""
    attempts: List[Dict[str, Any]] = []

    # Attempt 1: TABLE_STORAGE (provides row_count for most tables)
    sql1 = (
        f"SELECT table_name, row_count FROM `{BQ_PROJECT}.{BQ_DATASET}`.INFORMATION_SCHEMA.TABLE_STORAGE "
        "ORDER BY table_name"
    )
    log_step("Row counts attempt #1: INFORMATION_SCHEMA.TABLE_STORAGE …")
    res1 = run_query(sql1)
    attempts.append({"sql": sql1, "status": res1.get("status"), "error": res1.get("error_message")})
    if res1.get("status") == "success" and res1.get("rows"):
        res1["debug"] = {"attempts": attempts}
        return res1

    # Attempt 2: PARTITIONS (partitioned tables)
    sql2 = (
        f"SELECT table_name, SUM(row_count) AS row_count FROM `{BQ_PROJECT}.{BQ_DATASET}`.INFORMATION_SCHEMA.PARTITIONS "
        "GROUP BY table_name ORDER BY table_name"
    )
    log_step("Row counts attempt #2: INFORMATION_SCHEMA.PARTITIONS …")
    res2 = run_query(sql2)
    attempts.append({"sql": sql2, "status": res2.get("status"), "error": res2.get("error_message")})
    if res2.get("status") == "success" and res2.get("rows"):
        res2["debug"] = {"attempts": attempts}
        return res2

    # Attempt 3: Iterate COUNT(*) for each table (may be slower/costly)
    log_step("Row counts attempt #3: enumerate tables and COUNT(*) …")
    lt = list_tables()
    attempts.append({"step": "list_tables", "status": lt.get("status"), "error": lt.get("error_message")})
    if lt.get("status") != "success" or not lt.get("rows"):
        return {"status": "error", "error_message": lt.get("error_message", "Failed to list tables"), "debug": {"attempts": attempts}}

    rows_out: List[Dict[str, Any]] = []
    for r in lt.get("rows", []):
        t = r.get("table_name")
        if not t:
            continue
        sql3 = f"SELECT COUNT(*) AS row_count FROM `{BQ_PROJECT}.{BQ_DATASET}.{t}`"
        r3 = run_query(sql3)
        attempts.append({"sql": sql3, "status": r3.get("status"), "error": r3.get("error_message")})
        if r3.get("status") == "success" and r3.get("rows"):
            rows_out.append({"table_name": t, "row_count": r3["rows"][0]["row_count"]})

    return {
        "status": "success",
        "rows": rows_out,
        "num_rows": len(rows_out),
        "debug": {"attempts": attempts},
    }


