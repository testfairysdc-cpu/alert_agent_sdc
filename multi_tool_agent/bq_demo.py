import os
import sys
from pathlib import Path
import json
import argparse
from typing import Any, Dict, Optional

try:
    # Preferred: import from package when run as module
    from multi_tool_agent.bq_tools import query_bigquery
except ModuleNotFoundError:
    # Fallback: allow running as a file from repo root
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from multi_tool_agent.bq_tools import query_bigquery


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run a read-only BigQuery SQL using Application Default Credentials.\n"
            "Designed for Google Cloud Shell where GOOGLE_CLOUD_PROJECT is set."
        )
    )
    parser.add_argument(
        "--sql",
        type=str,
        default=(
            "SELECT 'Cloud Shell OK' AS message, CURRENT_TIMESTAMP() AS now"
        ),
        help="SQL to execute (must be a SELECT).",
    )
    parser.add_argument(
        "--project",
        type=str,
        default=os.getenv("GOOGLE_CLOUD_PROJECT"),
        help=(
            "GCP project id. Defaults to $GOOGLE_CLOUD_PROJECT when running in Cloud Shell."
        ),
    )
    parser.add_argument(
        "--location",
        type=str,
        default=os.getenv("BIGQUERY_LOCATION"),
        help=(
            "BigQuery location (e.g., US, EU). Defaults to $BIGQUERY_LOCATION if set."
        ),
    )
    parser.add_argument(
        "--params",
        type=str,
        default=None,
        help=(
            "Named parameters as JSON, e.g. '{\"event_type\": \"login\"}'."
        ),
    )
    parser.add_argument(
        "--max_rows",
        type=int,
        default=int(os.getenv("BIGQUERY_MAX_ROWS", "50")),
        help="Maximum rows to return (default 50).",
    )

    args = parser.parse_args()

    parameters: Optional[Dict[str, Any]] = None
    if args.params:
        parameters = json.loads(args.params)

    result = query_bigquery(
        sql=args.sql,
        project_id=args.project,
        location=args.location,
        parameters=parameters,
        maximum_rows=args.max_rows,
    )

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()


