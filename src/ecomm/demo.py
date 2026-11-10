"""Docker-free batch pipeline: generate -> ingest -> dual raw landing -> parity -> warehouse.

Everything runs locally and needs no cloud credentials:

- "s3-local" and "gcs-local" are two **local directories** standing in for the
  AWS S3 and Google Cloud Storage raw zones (labelled as substitutes; GCS is
  not running locally).
- DuckDB stands in for BigQuery.
- Kafka and Spark are not involved; the reference ingest logic in
  :mod:`ecomm.lake.ingest` plays the role of the Spark job.

Usage::

    uv run ecomm-demo --out data/demo --orders 300 --days 3
    uv run dbt build --project-dir dbt --profiles-dir dbt --target duckdb
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ecomm.contracts.types import format_utc
from ecomm.generator import GeneratorConfig, generate
from ecomm.generator.output import write_products_csv
from ecomm.lake.commit import BatchStatus, commit_batch, record_status
from ecomm.lake.ingest import IngestedEvent, classify, write_quarantine
from ecomm.lake.parity import reconcile_all
from ecomm.lake.raw import prepare_batch
from ecomm.lake.sources import messages_from_emissions
from ecomm.lake.stats import IngestStats, record_ingest_stats
from ecomm.storage.local import LocalObjectStore
from ecomm.warehouse.duckdb_client import DuckDBClient
from ecomm.warehouse.loader import WarehouseLoader

SOURCE = "commerce"


def _split(events: list[IngestedEvent], parts: int) -> list[list[IngestedEvent]]:
    ordered = sorted(events, key=lambda e: (e.envelope.event_time, str(e.envelope.event_id)))
    size = max(1, -(-len(ordered) // parts))
    return [ordered[i : i + size] for i in range(0, len(ordered), size)]


def run_batch_demo(config: GeneratorConfig, root: Path, batches: int = 3) -> dict[str, Any]:
    """Run the whole batch path under ``root`` and return an auditable summary."""
    root.mkdir(parents=True, exist_ok=True)
    s3 = LocalObjectStore(root / "lake" / "s3-local", name="s3-local")
    gcs = LocalObjectStore(root / "lake" / "gcs-local", name="gcs-local")
    control = LocalObjectStore(root / "lake" / "control", name="control")
    stores = {"s3-local": s3, "gcs-local": gcs}

    generated = generate(config)
    processed_at = config.end
    classified = classify(
        messages_from_emissions(generated.emissions),
        set(),
        processed_at=processed_at,
        late_threshold=timedelta(minutes=config.late_threshold_minutes),
    )
    run_id = f"demo-{config.seed}"
    quarantine_key = write_quarantine(s3, SOURCE, f"{run_id}-quarantine", classified.quarantined)
    record_ingest_stats(
        control, SOURCE, IngestStats.from_classified(run_id, processed_at, classified)
    )

    batch_results = []
    for index, events in enumerate(_split(classified.valid, batches)):
        batch = prepare_batch(
            events,
            source=SOURCE,
            run_id=run_id,
            batch_id=f"{run_id}-b{index:05d}",
            ingested_at=processed_at,
        )
        result = commit_batch(batch, stores)
        record_status(control, result)
        batch_results.append(result)
    if any(r.status is not BatchStatus.COMPLETE for r in batch_results):
        raise RuntimeError("not every batch reached COMPLETE; see the control ledger")

    parity = reconcile_all(stores, SOURCE)

    write_products_csv(generated, root / "products.csv")
    warehouse = root / "warehouse.duckdb"
    client = DuckDBClient(warehouse)
    try:
        loader = WarehouseLoader(client, s3, SOURCE)
        report = loader.backfill(config.start.date(), config.end.date(), f"{run_id}-backfill")
        report.write(control)
    finally:
        client.close()

    summary: dict[str, Any] = {
        "run_id": run_id,
        "window": [format_utc(config.start), format_utc(config.end)],
        "messages": len(generated.emissions),
        "valid_events": len(classified.valid),
        "duplicates_dropped": len(classified.duplicates),
        "quarantined": len(classified.quarantined),
        "late_events_kept": classified.late_count,
        "quarantine_object": quarantine_key,
        "batches": [r.to_json_dict() for r in batch_results],
        "parity_ok": all(p.ok for p in parity),
        "warehouse_rows_inserted": sum(r.rows_inserted for r in report.results),
        "warehouse_load_ok": report.ok,
        "warehouse_file": str(warehouse),
        "products_seed": str(root / "products.csv"),
        "note": (
            "s3-local/gcs-local are local directories standing in for S3/GCS; "
            "DuckDB stands in for BigQuery."
        ),
    }
    (root / "demo_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ecomm-demo", description=__doc__.split("\n")[0])
    parser.add_argument("--out", type=Path, default=Path("data/demo"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--orders", type=int, default=300)
    parser.add_argument("--days", type=float, default=3.0)
    parser.add_argument("--batches", type=int, default=3)
    args = parser.parse_args(argv)
    try:
        start = GeneratorConfig().start
        config = GeneratorConfig(
            seed=args.seed,
            orders=args.orders,
            start=start,
            end=start + timedelta(days=args.days),
            malformed_rate=0.02,
            late_rate=0.02,
            out_of_order_rate=0.05,
            duplicate_rate=0.03,
        )
    except ValidationError as exc:
        print(f"error: invalid arguments: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(run_batch_demo(config, args.out, args.batches), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
