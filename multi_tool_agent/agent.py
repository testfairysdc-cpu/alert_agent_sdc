import os
import datetime
from zoneinfo import ZoneInfo
from google.adk.agents import Agent
from multi_tool_agent.bq_tools import query_bigquery

def get_weather(city: str) -> dict:
    """Retrieves the current weather report for a specified city.

    Args:
        city (str): The name of the city for which to retrieve the weather report.

    Returns:
        dict: status and result or error msg.
    """
    if city.lower() == "new york":
        return {
            "status": "success",
            "report": (
                "The weather in New York is sunny with a temperature of 25 degrees"
                " Celsius (77 degrees Fahrenheit)."
            ),
        }
    else:
        return {
            "status": "error",
            "error_message": f"Weather information for '{city}' is not available.",
        }


def get_current_time(city: str) -> dict:
    """Returns the current time in a specified city.

    Args:
        city (str): The name of the city for which to retrieve the current time.

    Returns:
        dict: status and result or error msg.
    """

    if city.lower() == "new york":
        tz_identifier = "America/New_York"
    else:
        return {
            "status": "error",
            "error_message": (
                f"Sorry, I don't have timezone information for {city}."
            ),
        }

    tz = ZoneInfo(tz_identifier)
    now = datetime.datetime.now(tz)
    report = (
        f'The current time in {city} is {now.strftime("%Y-%m-%d %H:%M:%S %Z%z")}'
    )
    return {"status": "success", "report": report}


# BigQuery tool comes from bq_tools; keep the same public name available here


 


bigquery_agent = Agent(
    name="bigquery_agent",
    model="gemini-2.0-flash",
    description=(
        "Agent that can answer data questions by running read-only BigQuery queries."
    ),
    instruction=(
        "Use the provided tool to execute safe, read-only SELECT queries against BigQuery. "
        "Return concise summaries and include sample rows when helpful."
    ),
    tools=[query_bigquery],
)


def query_pgduty_summary(project_id: str = "ruckusoperations", location: str = "US") -> dict:
    sql = (
        """
SELECT
    'Simple Total Alerts Count' as analysis_type,
    COUNT(*) as total_alerts,
    COUNT(DISTINCT `Number`) as unique_incidents,
    COUNT(DISTINCT `Service`) as unique_services,
    COUNT(DISTINCT `Incident Type`) as unique_incident_types,
    MIN(`Created At America_Los_Angeles`) as earliest_alert,
    MAX(`Created At America_Los_Angeles`) as latest_alert
FROM `ruckusoperations.SDC1.pgduty`;
        """
    )
    return query_bigquery(sql=sql, project_id=project_id, location=location)


pgduty_summary_agent = Agent(
    name="pgduty_summary_agent",
    model="gemini-2.0-flash",
    description=(
        "Agent that returns a simple aggregate summary from ruckusoperations.SDC1.pgduty."
    ),
    instruction=(
        "Use the tool to fetch total alerts, unique incidents/services/types, and time range."
    ),
    tools=[query_pgduty_summary],
)


root_agent = Agent(
    name="weather_time_agent",
    model="gemini-2.0-flash",
    description=(
        "Agent that answers city time/weather questions and can run the pgduty summary in BigQuery."
    ),
    instruction=(
        "You can answer city time/weather questions. When the user asks for a pgduty alerts summary, "
        "call the pgduty summary tool to fetch totals, unique counts, and time range."
    ),
    tools=[get_weather, get_current_time, query_pgduty_summary],
)


router_agent = Agent(
    name="router_agent",
    model="gemini-2.0-flash",
    description=(
        "Top-level router that chooses the right sub-agent: weather/time vs BigQuery."
    ),
    instruction=(
        "If the user asks about weather or the current time in a city, use the weather/time tools. "
        "If the user asks for data analysis, SQL, BigQuery datasets/tables, or metrics, use the BigQuery tool. "
        "When using BigQuery, generate a safe, read-only SELECT and prefer parameterized queries. "
        "Summarize results succinctly and include a few sample rows when helpful."
    ),
    # Expose tools from both domains so the model can auto-select.
    tools=[get_weather, get_current_time, query_bigquery, query_pgduty_summary],
)


# Simple keyword-based router for chat services
PGDUTY_TRIGGERS = {
    "/pgduty",
    "pgduty",
    "pgduty summary",
    "query pgduty summary",
    "query_pgduty_summary",
}


def reply(user_message: str) -> str:
    text = (user_message or "").strip()
    lowered = text.lower()

    # Trigger pgduty summary explicitly by keywords
    if any(trigger in lowered for trigger in {t.lower() for t in PGDUTY_TRIGGERS}):
        location = os.getenv("BIGQUERY_LOCATION") or "US"
        result = query_pgduty_summary(project_id="ruckusoperations", location=location)
        return str(result)

    # Fallback: instruct user how to trigger pgduty or provide SQL
    return (
        "Unrecognized request. Use /pgduty or 'pgduty summary' to trigger the pgduty summary, "
        "or provide a BigQuery SQL query."
    )