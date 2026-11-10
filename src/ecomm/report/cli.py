"""``ecomm-report``: build the static business dashboard from the dbt marts.

uv run ecomm-demo --out data/demo
uv run dbt build ...
uv run ecomm-report --demo-dir data/demo      # writes data/demo/dashboard.html
"""

from __future__ import annotations

import argparse
from pathlib import Path

from ecomm.report.dashboard import DashboardError, build_dashboard
from ecomm.storage.local import LocalObjectStore
from ecomm.warehouse.client import utcnow
from ecomm.warehouse.duckdb_client import DuckDBClient


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ecomm-report", description="Build the HTML dashboard.")
    parser.add_argument("--demo-dir", type=Path, default=Path("data/demo"))
    parser.add_argument("--out", type=Path, help="default: <demo-dir>/dashboard.html")
    args = parser.parse_args(argv)

    warehouse = args.demo_dir / "warehouse.duckdb"
    if not warehouse.is_file():
        print(f"error: {warehouse} not found; run 'uv run ecomm-demo' first")
        return 2
    out = args.out or (args.demo_dir / "dashboard.html")
    client = DuckDBClient(warehouse, read_only=True)
    try:
        build_dashboard(
            client,
            out,
            utcnow(),
            LocalObjectStore(args.demo_dir / "lake" / "control", name="control"),
        )
    except DashboardError as exc:
        print(f"error: {exc}")
        return 1
    finally:
        client.close()
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
