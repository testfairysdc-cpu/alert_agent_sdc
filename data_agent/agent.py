from typing import Any, Dict

try:
    from google.adk.agents import Agent  # type: ignore
except Exception:  # noqa: BLE001
    Agent = None  # type: ignore

from data_agent.nl2sql import nl2sql_and_execute
from data_agent.nl2py import run_python_analysis
from data_agent.bq import list_tables, count_tables, table_row_counts


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


if Agent is not None:
    root_agent = Agent(
        name="data_agent",
        model="gemini-2.0-flash",
        description=(
            "Data agent that can translate natural language to BigQuery SQL and run Python analytics."
        ),
        instruction=(
            "When the user asks a data question, first try the NL2SQL tool to query BigQuery. "
            "For exploratory analysis or visualization, use the NL2Py tool to generate and execute Python."
        ),
        tools=[tool_nl2sql, tool_nl2py, tool_list_tables, tool_count_tables, tool_table_row_counts],
    )
else:
    # Fallback stub so imports don't fail in environments without ADK
    root_agent = {
        "name": "data_agent",
        "tools": [tool_nl2sql, tool_nl2py, tool_list_tables, tool_count_tables, tool_table_row_counts],
    }


