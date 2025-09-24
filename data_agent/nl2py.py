from typing import Any, Dict

import json

from data_agent.bq import run_query
from data_agent.config import BQ_PROJECT, BQ_DATASET


def _safe_exec_python(code: str, data: list[dict]) -> Dict[str, Any]:
    """Execute limited python code with a preloaded dataframe named df.

    This is a minimal sandbox; for production, consider stronger isolation.
    """
    try:
        import pandas as pd  # local import
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "error_message": f"pandas missing: {exc}"}

    df = pd.DataFrame(data)
    safe_builtins = {"len": len, "range": range, "min": min, "max": max, "sum": sum}
    globals_dict: dict[str, Any] = {"__builtins__": safe_builtins, "pd": pd}
    locals_dict: dict[str, Any] = {"df": df}
    try:
        exec(code, globals_dict, locals_dict)  # noqa: S102
        # Convention: analysis result placed in variable `result`
        result = locals_dict.get("result", None)
        # Optional plot saved as path in `figure_path`
        figure_path = locals_dict.get("figure_path", None)
        return {
            "status": "success",
            "result": result,
            "figure_path": figure_path,
        }
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "error_message": str(exc)}


def run_python_analysis(question: str, *, table: str | None = None, limit: int = 200) -> Dict[str, Any]:
    """Generate simple Python analysis code and execute it against a sample table.

    If Vertex SDK is available, we can ask the model to emit Python using pandas/matplotlib.
    """
    if not question or not isinstance(question, str):
        return {"status": "error", "error_message": "question must be a non-empty string"}

    # Pick a table if not provided: list tables via INFORMATION_SCHEMA and choose the first.
    target_table = table
    if target_table is None:
        meta = run_query(
            f"SELECT table_name FROM `{BQ_PROJECT}.{BQ_DATASET}`.INFORMATION_SCHEMA.TABLES ORDER BY table_name LIMIT 1"
        )
        if meta.get("status") != "success" or not meta.get("rows"):
            return {"status": "error", "error_message": "Cannot determine a table to sample."}
        target_table = meta["rows"][0]["table_name"]

    # Pull sample rows
    sample = run_query(f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{target_table}` LIMIT {int(limit)}")
    if sample.get("status") != "success":
        return sample

    # Try to generate code with a model; otherwise use a basic default
    try:
        from vertexai.generative_models import GenerativeModel  # type: ignore

        schema_preview = list(sample.get("schema", []))
        prompt = (
            "You are a data analyst. Given a dataframe `df`, write concise Python code using pandas "
            "(and optionally matplotlib) to answer the question. Put the final answer in a variable named `result`. "
            "If you plot, save the figure to a temp path and set `figure_path` to that path. Do not print.\n\n"
            f"Question: {question}\nSchema: {json.dumps(schema_preview)}\n"
        )
        model = GenerativeModel("gemini-1.5-flash")
        resp = model.generate_content(prompt)
        text = resp.text if hasattr(resp, "text") else str(resp)
        code = text
        if "```" in text:
            for part in text.split("```"):
                if "import" in part or "df" in part:
                    code = part.strip()
                    break
    except Exception:
        code = (
            "# Default analysis: basic column overview\n"
            "result = {'num_rows': len(df), 'columns': list(df.columns)}\n"
        )

    return _safe_exec_python(code, sample.get("rows", []))


