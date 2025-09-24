import os
from typing import Any, Dict, List, Optional

from google.cloud import bigquery


def query_bigquery(
    sql: str,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    maximum_rows: int = 1000,
) -> dict:
    """Run a read-only BigQuery SQL and return rows as a list of dicts.

    Args:
        sql: The SQL query to execute (should be a SELECT).
        project_id: Optional GCP project id. If None, uses env or ADC default.
        location: Optional BigQuery location (e.g., "US", "EU").
        parameters: Optional dict of named query parameters.
        maximum_rows: Max number of rows to return for safety.

    Returns:
        dict: status, rows, schema, num_rows or error.
    """
    try:
        if not isinstance(sql, str) or not sql.strip():
            return {
                "status": "error",
                "error_message": "SQL must be a non-empty string.",
            }

        lowered = sql.strip().lower()
        if not lowered.startswith("select"):
            return {
                "status": "error",
                "error_message": "Only read-only SELECT queries are allowed.",
            }

        effective_project = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        client = bigquery.Client(project=effective_project)

        job_config = bigquery.QueryJobConfig()

        if parameters:
            query_parameters: List[bigquery.ScalarQueryParameter] = []
            for name, value in parameters.items():
                if isinstance(value, bool):
                    bq_type = "BOOL"
                elif isinstance(value, int):
                    bq_type = "INT64"
                elif isinstance(value, float):
                    bq_type = "FLOAT64"
                else:
                    bq_type = "STRING"
                query_parameters.append(
                    bigquery.ScalarQueryParameter(name, bq_type, value)
                )
            job_config.query_parameters = query_parameters

        query_job = client.query(sql, job_config=job_config, location=location)
        result = query_job.result()

        rows: List[Dict[str, Any]] = []
        field_names = [field.name for field in result.schema]
        for row in result:
            row_dict = {field_name: row[field_name] for field_name in field_names}
            rows.append(row_dict)

        if maximum_rows is not None and maximum_rows > 0:
            rows = rows[:maximum_rows]

        schema = [
            {
                "name": field.name,
                "type": field.field_type,
                "mode": getattr(field, "mode", None),
            }
            for field in result.schema
        ]

        return {
            "status": "success",
            "rows": rows,
            "num_rows": len(rows),
            "schema": schema,
            "job_id": query_job.job_id,
        }
    except Exception as exc:  # noqa: BLE001 - surface runtime errors
        return {"status": "error", "error_message": str(exc)}


