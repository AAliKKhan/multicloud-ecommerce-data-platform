"""``ecomm-metrics``: expose pipeline metrics for Prometheus.

# one-shot: write the Prometheus text format to a file (e.g. for a textfile collector)
uv run ecomm-metrics --demo-dir data/demo --textfile data/demo/metrics.prom

# long-running: serve http://localhost:9108/metrics, recomputed on every scrape
uv run ecomm-metrics --demo-dir data/demo --serve 9108
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Iterable
from datetime import timedelta
from pathlib import Path

from prometheus_client import CollectorRegistry, generate_latest, start_http_server
from prometheus_client.core import Metric
from prometheus_client.registry import Collector

from ecomm.demo import SOURCE
from ecomm.observability.metrics import (
    Snapshot,
    build_registry,
    collect_snapshot,
    snapshot_to_families,
)
from ecomm.quality.checks import QualityContext
from ecomm.storage.local import LocalObjectStore
from ecomm.warehouse.client import utcnow
from ecomm.warehouse.duckdb_client import DuckDBClient


def snapshot_for_demo_dir(root: Path) -> Snapshot:
    """Collect a snapshot from a demo directory (read-only warehouse access)."""
    s3 = LocalObjectStore(root / "lake" / "s3-local", name="s3-local")
    gcs = LocalObjectStore(root / "lake" / "gcs-local", name="gcs-local")
    control = LocalObjectStore(root / "lake" / "control", name="control")
    client = DuckDBClient(root / "warehouse.duckdb", read_only=True)
    try:
        ctx = QualityContext(
            client=client,
            source=SOURCE,
            now=utcnow(),
            primary_store=s3,
            lake_stores={"s3-local": s3, "gcs-local": gcs},
            control_store=control,
            freshness_sla=timedelta(hours=24),
        )
        return collect_snapshot(ctx)
    finally:
        client.close()


class _LiveCollector(Collector):
    """Recomputes the snapshot on every scrape so values are never stale."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def collect(self) -> Iterable[Metric]:
        return snapshot_to_families(snapshot_for_demo_dir(self._root))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ecomm-metrics", description=__doc__.split("\n")[0])
    parser.add_argument("--demo-dir", type=Path, default=Path("data/demo"))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--textfile", type=Path, help="write Prometheus text format and exit")
    mode.add_argument("--serve", type=int, metavar="PORT", help="serve /metrics on this port")
    args = parser.parse_args(argv)

    if not (args.demo_dir / "warehouse.duckdb").is_file():
        print(
            f"error: {args.demo_dir / 'warehouse.duckdb'} not found; run 'uv run ecomm-demo' first"
        )
        return 2

    if args.textfile is not None:
        registry = build_registry(snapshot_for_demo_dir(args.demo_dir))
        args.textfile.parent.mkdir(parents=True, exist_ok=True)
        args.textfile.write_bytes(generate_latest(registry))
        print(f"wrote {args.textfile}")
        return 0

    live = CollectorRegistry()
    live.register(_LiveCollector(args.demo_dir))
    start_http_server(args.serve, registry=live, addr="127.0.0.1")  # loopback only
    print(f"serving metrics on http://127.0.0.1:{args.serve}/metrics (Ctrl+C to stop)")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
