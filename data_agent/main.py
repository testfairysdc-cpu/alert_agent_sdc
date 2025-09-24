import argparse
import json

from data_agent.nl2sql import nl2sql_and_execute
from data_agent.nl2py import run_python_analysis
from data_agent.bq import list_tables, count_tables, table_row_counts


def main() -> None:
    parser = argparse.ArgumentParser("data_agent entrypoint")
    sub = parser.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("nl2sql", help="Translate NL to SQL and execute")
    s1.add_argument("question", type=str)
    s1.add_argument("--max_rows", type=int, default=100)

    s2 = sub.add_parser("nl2py", help="Generate+run Python analysis against a sample table")
    s2.add_argument("question", type=str)
    s2.add_argument("--limit", type=int, default=200)

    s3 = sub.add_parser("tables", help="List tables in configured dataset")
    s4 = sub.add_parser("tables-count", help="Count tables in configured dataset")
    s5 = sub.add_parser("table-rows", help="Approx row counts for each table via INFORMATION_SCHEMA")

    args = parser.parse_args()

    if args.cmd == "nl2sql":
        res = nl2sql_and_execute(args.question, maximum_rows=args.max_rows)
        print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
        return

    if args.cmd == "nl2py":
        res = run_python_analysis(args.question, limit=args.limit)
        print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
        return

    if args.cmd == "tables":
        res = list_tables()
        print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
        return

    if args.cmd == "tables-count":
        res = count_tables()
        print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
        return

    if args.cmd == "table-rows":
        res = table_row_counts()
        print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
        return


if __name__ == "__main__":
    main()


