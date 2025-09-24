from typing import Any, Dict

from data_agent.config import (
    BQ_PROJECT,
    BQ_DATASET,
    GEN_TEMPERATURE,
    GEN_MAX_TOKENS,
)
from data_agent.bq import (
    run_query,
    get_schema_summary,
    get_tables_and_columns,
    list_tables,
    count_tables,
    table_row_counts,
)


def _generate_sql_with_model(question: str, schema_context: str) -> str:
    try:
        # Prefer Vertex AI GenerativeModel if available
        from vertexai.generative_models import GenerativeModel  # type: ignore

        system = (
            "You translate natural language to BigQuery Standard SQL strictly for the dataset "
            f"`{BQ_PROJECT}.{BQ_DATASET}`. Use fully-qualified table names with backticks. "
            "Never write DML/DDL. Always include a LIMIT unless explicitly asked for full results."
        )
        prompt = (
            f"Schema (tables -> columns):\n{schema_context}\n\n"
            f"User question: {question}\n\n"
            "Return only the SQL query in a code block."
        )
        model = GenerativeModel("gemini-1.5-flash")
        resp = model.generate_content(
            [system, prompt],
            generation_config={
                "temperature": GEN_TEMPERATURE,
                "max_output_tokens": GEN_MAX_TOKENS,
            },
        )
        text = resp.text if hasattr(resp, "text") else str(resp)
    except Exception:
        # Fallback: naive template (very limited)
        text = (
            f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.YOUR_TABLE` LIMIT 50"
        )

    # Extract SQL from a code block if present
    sql = text
    if "```" in text:
        parts = text.split("```")
        # find first block that looks like SQL
        for p in parts:
            if "select" in p.lower():
                sql = p.strip()
                break
    return sql.strip()


def _sanitize_sql(sql: str) -> str:
    lowered = sql.strip().lower()
    if not lowered.startswith("select"):
        raise ValueError("Generated SQL must be a SELECT query.")
    # If dataset not included, try to auto-qualify naive FROM/JOIN bare identifiers
    must = f"`{BQ_PROJECT}.{BQ_DATASET}."
    if must not in sql:
        # naive auto-qualify for patterns: FROM pgduty / JOIN pgduty
        replacements = {
            " from ": f" FROM `{BQ_PROJECT}.{BQ_DATASET}.",
            " join ": f" JOIN `{BQ_PROJECT}.{BQ_DATASET}.",
        }
        sql_work = sql
        for key, val in replacements.items():
            sql_work = sql_work.replace(key, val)
            sql_work = sql_work.replace(key.upper(), val)
        if must not in sql_work:
            raise ValueError(
                f"SQL must reference `{BQ_PROJECT}.{BQ_DATASET}` with fully-qualified table names."
            )
        sql = sql_work
    return sql


def nl2sql_and_execute(question: str, *, maximum_rows: int = 100) -> Dict[str, Any]:
    if not question or not isinstance(question, str):
        return {"status": "error", "error_message": "question must be a non-empty string"}
    # Intent detection (CN/EN) for meta-queries
    q = (question or "").strip().lower()
    if any(k in q for k in ["how many tables", "count tables", "多少表", "有多少表"]):
        res = count_tables()
        res.setdefault("debug", {})["intent"] = "count_tables"
        return res
    if any(k in q for k in ["list tables", "tables list", "有哪些表", "列出表"]):
        res = list_tables()
        res.setdefault("debug", {})["intent"] = "list_tables"
        return res
    if any(k in q for k in ["each table", "per table", "每张表", "每个表"]):
        res = table_row_counts()
        res.setdefault("debug", {})["intent"] = "table_row_counts"
        return res

    # Richer schema context: table -> columns with types (truncated to keep prompt small)
    try:
        toc = get_tables_and_columns()
        schema = "\n".join(
            f"{t} -> " + ", ".join(f"{c['column_name']}:{c['data_type']}" for c in cols)
            for t, cols in list(toc.items())
        )
        schema_preview = "\n".join(schema.splitlines()[:25])
    except Exception:
        schema = get_schema_summary()
        schema_preview = "\n".join(schema.splitlines()[:25])

    generated = _generate_sql_with_model(question, schema)
    try:
        sanitized = _sanitize_sql(generated)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "error",
            "error_message": f"SQL sanitize failed: {exc}",
            "sql": generated,
            "debug": {"intent": "nl2sql", "schema_preview": schema_preview},
        }

    dry = run_query(sanitized, dry_run=True)
    if dry.get("status") != "success":
        return {
            "status": "error",
            "error_message": dry.get("error_message"),
            "sql": sanitized,
            "debug": {"intent": "nl2sql", "schema_preview": schema_preview},
        }

    exec_res = run_query(sanitized, maximum_rows=maximum_rows)
    exec_res.setdefault("debug", {})
    exec_res["debug"].update(
        {
            "intent": "nl2sql",
            "schema_preview": schema_preview,
            "generated_sql": generated,
            "sanitized_sql": sanitized,
            "dry_run_bytes": dry.get("total_bytes_processed"),
        }
    )
    return exec_res


