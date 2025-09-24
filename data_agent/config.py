import os
from typing import Optional

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # dotenv is optional; environment variables still work without it
    pass


def get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else (default or "")


# BigQuery project/dataset/location
BQ_PROJECT = get_env("BQ_PROJECT", "ruckusoperations")
BQ_DATASET = get_env("BQ_DATASET", "SDC1")
BQ_LOCATION = get_env("BQ_LOCATION", "US")

# Optional allow cross-dataset queries (default False)
ALLOW_CROSS_DATASET = get_env("ALLOW_CROSS_DATASET", "false").lower() in {"1", "true", "yes", "on"}

# NL2SQL generation settings
GEN_TEMPERATURE = float(get_env("GEN_TEMPERATURE", "0.2"))
GEN_MAX_TOKENS = int(get_env("GEN_MAX_TOKENS", "1024"))
GEN_TOP_P = float(get_env("GEN_TOP_P", "0.95"))
GEN_TOP_K = int(get_env("GEN_TOP_K", "40"))

# Vertex SDK
VERTEX_PROJECT = get_env("VERTEX_PROJECT", BQ_PROJECT)
VERTEX_LOCATION = get_env("VERTEX_LOCATION", "us-central1")
VERTEX_MODEL_NAME = get_env("VERTEX_MODEL_NAME", "gemini-1.5-flash")


