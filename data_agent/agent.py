from typing import Any, Dict

try:
    from google.adk.agents import Agent  # type: ignore
except Exception:  # noqa: BLE001
    Agent = None  # type: ignore

from data_agent.nl2sql import nl2sql_and_execute
from data_agent.nl2py import run_python_analysis
from data_agent.bq import list_tables, count_tables, table_row_counts
from data_agent.utils.log import log_step


def tool_nl2sql(question: str) -> Dict[str, Any]:
    return nl2sql_and_execute(question)


def tool_nl2py(question: str) -> Dict[str, Any]:
    return run_python_analysis(question)


def tool_list_tables() -> Dict[str, Any]:
    return list_tables()


def tool_count_tables() -> Dict[str, Any]:
    return count_tables()


def tool_table_row_counts() -> Dict[str, Any]:
    return table_row_counts()


def tool_answer(question: str) -> Dict[str, Any]:
    """High-level orchestrator: understand intent, run, and return a readable answer with steps."""
    log_step("[answer] Routing question via NL2SQL/intents …")
    res = nl2sql_and_execute(question)

    steps = []
    dbg = res.get("debug", {}) if isinstance(res, dict) else {}
    intent = dbg.get("intent", "nl2sql")
    steps.append(f"Intent: {intent}")

    if "generated_sql" in dbg or "sanitized_sql" in dbg or "sql" in res:
        steps.append("Generated/selected SQL ready to run")
        if dbg.get("sanitized_sql"):
            steps.append(f"SQL: {dbg.get('sanitized_sql')}")
        elif res.get("sql"):
            steps.append(f"SQL: {res.get('sql')}")

    if "dry_run_bytes" in dbg:
        steps.append(f"Dry run bytes: {dbg.get('dry_run_bytes')}")

    if res.get("status") == "success":
        steps.append("Execution: success")
        preview = res.get("rows", [])[:5]
        message = "\n".join([f"- {s}" for s in steps])
        return {
            "status": "success",
            "message": message,
            "num_rows": res.get("num_rows"),
            "rows_preview": preview,
            "schema": res.get("schema"),
        }
    else:
        steps.append("Execution: failed")
        message = "\n".join([f"- {s}" for s in steps])
        return {
            "status": "error",
            "message": message,
            "error_message": res.get("error_message"),
            "sql": res.get("sql"),
        }


if Agent is not None:
    root_agent = Agent(
        name="data_agent",
        model="gemini-2.0-flash",
        description=(
            "Data agent that can translate natural language to BigQuery SQL and run Python analytics."
        ),
        instruction=(
            "When the user asks a data question, first use the answer tool to route and execute. "
            "Show a short step list of what you are doing (intent, SQL, dry-run, execute). "
            "You can still call NL2SQL or NL2Py directly for advanced cases, but prefer the answer tool."
        ),
        tools=[tool_answer, tool_nl2sql, tool_nl2py, tool_list_tables, tool_count_tables, tool_table_row_counts],
    )
else:
    # Fallback stub so imports don't fail in environments without ADK
    root_agent = {
        "name": "data_agent",
        "tools": [tool_answer, tool_nl2sql, tool_nl2py, tool_list_tables, tool_count_tables, tool_table_row_counts],
    }


