"""``ecomm-metrics``: expose pipeline metrics for Prometheus.

# one-shot: write the Prometheus text format to a file (e.g. for a textfile collector)
uv run ecomm-metrics --demo-dir data/demo --textfile data/demo/metrics.prom

# long-running: serve http://localhost:9108/metrics, recomputed on every scrape
uv run ecomm-metrics --demo-dir data/demo --serve 9108

# in a container: listen on every interface (the published port is loopback-only)
uv run ecomm-metrics --demo-dir /data/pipeline --serve 9108 --addr 0.0.0.0
"""

from __future__ import annotations

import argparse
import logging
import threading
import time
from collections.abc import Callable, Iterable
from datetime import timedelta
from pathlib import Path

from prometheus_client import CollectorRegistry, generate_latest, start_http_server
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from ecomm.demo import SOURCE
from ecomm.logging_utils import get_logger, log
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


def snapshot_for_demo_dir(root: Path, parity: str = "full") -> Snapshot:
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
        return collect_snapshot(ctx, parity)
    finally:
        client.close()


def families_or_error(root: Path, parity: str = "full") -> list[Metric]:
    """All metric families for one refresh, or a minimal "not ready" answer.

    The warehouse may not exist yet, or a writer (a load, dbt) may hold DuckDB's file lock. That
    must never turn into a failed scrape with no information: the exporter stays up and says
    ``ecomm_exporter_ready 0`` with the reason class, so dashboards and alerts can show it.
    """
    try:
        return snapshot_to_families(snapshot_for_demo_dir(root, parity)) + [
            _ready_family(1.0, ""),
        ]
    except Exception as exc:  # DuckDB IOException (lock), missing file, corrupt data ...
        log(
            get_logger("ecomm.exporter"),
            logging.WARNING,
            "snapshot failed",
            error_type=type(exc).__name__,
        )
        return [_ready_family(0.0, type(exc).__name__)]


def _ready_family(value: float, reason: str) -> Metric:
    family = GaugeMetricFamily(
        "ecomm_exporter_ready",
        "1 when the pipeline data could be read for this scrape; reason is an exception class.",
        labels=["reason"],
    )
    family.add_metric([reason], value)
    return family


class CachedCollector(Collector):
    """Serves the last snapshot, refreshed by a background thread.

    Collecting the metrics reads the warehouse and walks the lake, which can take seconds to
    minutes on a large lake or a slow bind mount. A scrape must never wait for that (Prometheus
    would time out and the health check would flap), so ``collect`` returns instantly from memory
    and says how old the data is. Before the first refresh finishes it answers
    ``ecomm_exporter_ready{reason="starting"} 0``.
    """

    def __init__(
        self,
        root: Path,
        refresh_seconds: float = 60.0,
        parity: str = "light",
        clock: Callable[[], float] = time.monotonic,
        compute: Callable[[Path, str], list[Metric]] = families_or_error,
    ) -> None:
        self._root = root
        self._refresh_seconds = refresh_seconds
        self._parity = parity
        self._clock = clock
        self._compute = compute
        self._lock = threading.Lock()
        self._families: list[Metric] = [_ready_family(0.0, "starting")]
        self._updated_at: float | None = None
        self._duration: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def refresh(self) -> None:
        started = self._clock()
        families = self._compute(self._root, self._parity)
        finished = self._clock()
        with self._lock:
            self._families = families
            self._updated_at = finished
            self._duration = finished - started

    def start(self) -> None:
        def loop() -> None:
            while not self._stop.is_set():
                try:
                    self.refresh()
                except Exception as exc:  # never let the refresher die silently
                    log(
                        get_logger("ecomm.exporter"),
                        logging.ERROR,
                        "refresh crashed",
                        error_type=type(exc).__name__,
                    )
                self._stop.wait(self._refresh_seconds)

        self._thread = threading.Thread(target=loop, name="snapshot-refresher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def collect(self) -> Iterable[Metric]:
        with self._lock:
            families = list(self._families)
            updated, duration = self._updated_at, self._duration
        if updated is not None:
            age = GaugeMetricFamily(
                "ecomm_exporter_snapshot_age_seconds",
                "Seconds since the served snapshot was computed.",
                value=max(0.0, self._clock() - updated),
            )
            took = GaugeMetricFamily(
                "ecomm_exporter_refresh_duration_seconds",
                "Seconds the last snapshot took to compute.",
                value=duration or 0.0,
            )
            families += [age, took]
        return families


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ecomm-metrics", description=__doc__.split("\n")[0])
    parser.add_argument("--demo-dir", type=Path, default=Path("data/demo"))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--textfile", type=Path, help="write Prometheus text format and exit")
    mode.add_argument("--serve", type=int, metavar="PORT", help="serve /metrics on this port")
    parser.add_argument(
        "--addr",
        default="127.0.0.1",
        help="address for --serve (default: loopback; use 0.0.0.0 inside a container)",
    )
    parser.add_argument(
        "--refresh-seconds",
        type=float,
        default=60.0,
        help="with --serve: how often the snapshot is recomputed in the background",
    )
    parser.add_argument(
        "--parity",
        choices=["light", "full"],
        default="light",
        help="with --serve: 'light' compares manifests and object presence (cheap); 'full' "
        "also re-reads and hashes every object (slow on big lakes)",
    )
    args = parser.parse_args(argv)

    # One-shot export needs the data now. The long-running server does not: it starts anyway and
    # reports ecomm_exporter_ready 0 until the pipeline has produced a warehouse.
    if args.textfile is not None and not (args.demo_dir / "warehouse.duckdb").is_file():
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

    collector = CachedCollector(args.demo_dir, args.refresh_seconds, parity=args.parity)
    collector.start()
    live = CollectorRegistry()
    live.register(collector)
    start_http_server(args.serve, registry=live, addr=args.addr)
    print(
        f"serving metrics on http://{args.addr}:{args.serve}/metrics (Ctrl+C to stop)", flush=True
    )
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
