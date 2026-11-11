"""``ecomm-reconcile``: compare every raw batch across the two destinations.

    uv run ecomm-reconcile --demo-dir data/spark
    uv run ecomm-reconcile --demo-dir data/spark --json

For every batch known to either destination this re-reads the objects, verifies them against
their own manifest (SHA-256 and record counts) and then compares the destinations on event
count and the checksum of the set of event IDs. Exit code 0 = all batches match, 1 = at least
one batch differs or is missing on one side, 2 = the lake directories do not exist. The report
is also written to the control store (``reports/reconciliation/<run_id>.json``).

Local substitutes: ``s3-local`` and ``gcs-local`` are directories standing in for S3 and GCS.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ecomm.contracts.types import format_utc
from ecomm.lake.parity import ParityReport, reconcile_all
from ecomm.storage.local import LocalObjectStore
from ecomm.warehouse.client import utcnow


def report_to_dict(run_id: str, reports: list[ParityReport]) -> dict[str, Any]:
    mismatched = [r for r in reports if not r.ok]
    return {
        "run_id": run_id,
        "checked_at": format_utc(utcnow()),
        "ok": not mismatched,
        "batches_checked": len(reports),
        "batches_mismatched": len(mismatched),
        "batches": [
            {
                "batch_id": r.batch_id,
                "status": r.status.value,
                "problems": list(r.problems),
                "destinations": {
                    name: {
                        "manifest_found": c.manifest_found,
                        "event_count": c.event_count,
                        "logical_checksum": c.logical_checksum,
                    }
                    for name, c in sorted(r.destinations.items())
                },
            }
            for r in reports
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ecomm-reconcile", description=__doc__.split("\n")[0])
    parser.add_argument("--demo-dir", type=Path, default=Path("data/demo"))
    parser.add_argument("--source", default="commerce")
    parser.add_argument("--run-id", help="label for the report (default: from the clock)")
    parser.add_argument("--json", action="store_true", help="print the full report as JSON")
    args = parser.parse_args(argv)

    root: Path = args.demo_dir / "lake"
    for name in ("s3-local", "gcs-local"):
        if not (root / name).is_dir():
            print(f"error: {root / name} not found; produce a lake first (ecomm-demo or Spark)")
            return 2
    run_id = args.run_id or f"reconcile-{utcnow():%Y%m%dT%H%M%SZ}"
    stores = {
        "s3-local": LocalObjectStore(root / "s3-local", name="s3-local"),
        "gcs-local": LocalObjectStore(root / "gcs-local", name="gcs-local"),
    }
    reports = reconcile_all(stores, args.source)
    result = report_to_dict(run_id, reports)
    control = LocalObjectStore(root / "control", name="control")
    control.write_object(
        f"reports/reconciliation/{run_id}.json", (json.dumps(result, indent=2) + "\n").encode()
    )

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(
            f"reconciliation {run_id}: {result['batches_checked']} batches, "
            f"{result['batches_mismatched']} mismatched"
        )
        for batch in result["batches"]:
            if batch["problems"]:
                print(f"  {batch['batch_id']}: {'; '.join(batch['problems'][:3])}")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
