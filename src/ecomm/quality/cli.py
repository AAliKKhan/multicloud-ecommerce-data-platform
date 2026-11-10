"""``ecomm-quality``: run the data-quality suite against a demo directory.

    uv run ecomm-demo --out data/demo
    uv run dbt build ...                      # marts must exist for the mart checks
    uv run ecomm-quality --demo-dir data/demo

Exit code 0 = no failed checks (warnings allowed), 1 = at least one FAIL.
"""

from __future__ import annotations

import argparse
import json
from datetime import timedelta
from pathlib import Path

from ecomm.demo import SOURCE
from ecomm.quality.checks import QualityContext
from ecomm.quality.runner import persist_report, run_quality_suite
from ecomm.storage.local import LocalObjectStore
from ecomm.warehouse.client import utcnow
from ecomm.warehouse.duckdb_client import DuckDBClient


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ecomm-quality", description=__doc__.split("\n")[0])
    parser.add_argument("--demo-dir", type=Path, default=Path("data/demo"))
    parser.add_argument("--sla-hours", type=float, default=24.0, help="freshness SLA")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    parser.add_argument("--no-persist", action="store_true", help="do not store results")
    args = parser.parse_args(argv)

    root: Path = args.demo_dir
    warehouse = root / "warehouse.duckdb"
    if not warehouse.is_file():
        print(f"error: {warehouse} not found; run 'uv run ecomm-demo' first")
        return 2

    s3 = LocalObjectStore(root / "lake" / "s3-local", name="s3-local")
    gcs = LocalObjectStore(root / "lake" / "gcs-local", name="gcs-local")
    control = LocalObjectStore(root / "lake" / "control", name="control")
    client = DuckDBClient(warehouse)
    try:
        ctx = QualityContext(
            client=client,
            source=SOURCE,
            now=utcnow(),
            primary_store=s3,
            lake_stores={"s3-local": s3, "gcs-local": gcs},
            control_store=control,
            freshness_sla=timedelta(hours=args.sla_hours),
        )
        report = run_quality_suite(ctx)
        if not args.no_persist:
            persist_report(report, client, control, SOURCE)
    finally:
        client.close()

    print(json.dumps(report.to_json_dict(), indent=2) if args.json else report.format_text())
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
