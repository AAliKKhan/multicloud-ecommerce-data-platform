"""``ecomm-load``: load a raw lake directory into the local DuckDB warehouse.

Works on any directory with the lake layout, whether it was produced by
``ecomm-demo`` or by the Spark streaming job (``data/spark``):

    uv run ecomm-load --demo-dir data/spark
    uv run ecomm-load --demo-dir data/spark --start 2026-11-01 --end 2026-11-03
    uv run ecomm-load --demo-dir data/spark --only-new   # skip batches already loaded

Only *committed* batches (those with a manifest) are read. The load is idempotent: running
it again inserts nothing new. Prints a JSON report and exits 1 if any batch failed.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

from ecomm.orchestration.ids import InvalidRunIdError, sanitize_run_id
from ecomm.storage.local import LocalObjectStore
from ecomm.warehouse.client import utcnow
from ecomm.warehouse.duckdb_client import DuckDBClient
from ecomm.warehouse.loader import WarehouseLoader


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ecomm-load", description=__doc__.split("\n")[0])
    parser.add_argument("--demo-dir", type=Path, default=Path("data/demo"))
    parser.add_argument("--source", default="commerce")
    parser.add_argument("--start", type=date.fromisoformat, help="first date (default: 2000-01-01)")
    parser.add_argument("--end", type=date.fromisoformat, help="last date (default: tomorrow)")
    parser.add_argument("--run-id", help="label for the backfill report (default: from the clock)")
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="skip batches whose latest load attempt succeeded (a cheap incremental run; "
        "without it every committed batch is re-checked, which is also safe)",
    )
    args = parser.parse_args(argv)

    lake = args.demo_dir / "lake" / "s3-local"
    if not lake.is_dir():
        print(f"error: {lake} not found; produce a lake first (ecomm-demo or the Spark job)")
        return 2
    start = args.start or date(2000, 1, 1)
    end = args.end or (utcnow().date() + timedelta(days=1))
    try:
        run_id = sanitize_run_id(args.run_id or f"load-{utcnow():%Y%m%dT%H%M%SZ}")
    except InvalidRunIdError as exc:
        print(f"error: {exc}")
        return 2

    if end < start:
        print(f"error: --end {end} is before --start {start}")
        return 2

    client = DuckDBClient(args.demo_dir / "warehouse.duckdb")
    try:
        report = WarehouseLoader(
            client, LocalObjectStore(lake, name="s3-local"), args.source
        ).backfill(start, end, run_id, skip_succeeded=args.only_new)
        report.write(LocalObjectStore(args.demo_dir / "lake" / "control", name="control"))
    finally:
        client.close()
    print(json.dumps(report.to_json_dict(), indent=2))
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
