import os
import sys


def log_step(message: str) -> None:
    if os.getenv("DATA_AGENT_VERBOSE", "1") not in {"0", "false", "False"}:
        print(f"[data_agent] {message}", file=sys.stderr)


