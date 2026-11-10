"""Idempotent raw-to-warehouse loader.

Per committed batch (a batch is committed once its manifest exists):

1. Read the manifest's objects and verify each SHA-256 against it.
2. Check ``rows_read == manifest.event_count`` (source-to-target reconciliation).
3. Bulk-load rows into a per-batch **staging table** with an explicit schema.
4. ``MERGE`` staging into ``raw_events`` on ``event_id`` (insert only when not
   matched), with a static ``event_date`` range in the join so BigQuery can
   prune partitions.
5. Verify every staged event is present in the target, drop staging, and write
   an audit row (counts, status, error).

Replaying a batch inserts nothing (all rows are counted as duplicates), so
business totals cannot be inflated. Raw files are never modified, so a failed
load is simply retried. Atomicity note: staging + MERGE is a single warehouse
statement for the target, so a crash leaves either all or none of a batch's
new rows in ``raw_events``.
"""

from __future__ import annotations

import contextlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any

from ecomm.lake.manifest import BatchManifest, manifest_key, sha256_hex
from ecomm.lake.parity import list_batch_ids
from ecomm.storage.interface import ObjectStore, StorageError
from ecomm.warehouse.client import WarehouseClient, WarehouseError, utcnow
from ecomm.warehouse.schema import LOAD_AUDIT, RAW_EVENTS, sanitize_for_identifier

_SAFE_VALUE = re.compile(r"^[A-Za-z0-9_.:=-]+$")


def _literal(value: str) -> str:
    if not _SAFE_VALUE.fullmatch(value):
        raise ValueError(f"unsafe value for SQL literal: {value!r}")
    return f"'{value}'"


class LoadStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class LoadResult:
    batch_id: str
    load_id: str
    status: LoadStatus
    expected_rows: int
    rows_read: int = 0
    rows_inserted: int = 0
    rows_duplicate: int = 0
    error: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "load_id": self.load_id,
            "status": self.status.value,
            "expected_rows": self.expected_rows,
            "rows_read": self.rows_read,
            "rows_inserted": self.rows_inserted,
            "rows_duplicate": self.rows_duplicate,
            "error": self.error,
        }


@dataclass(frozen=True)
class BackfillReport:
    run_id: str
    start: date
    end: date
    results: list[LoadResult] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(r.status is LoadStatus.SUCCEEDED for r in self.results)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "ok": self.ok,
            "batches_loaded": len(self.results),
            "batches_skipped": self.skipped,
            "rows_read": sum(r.rows_read for r in self.results),
            "rows_inserted": sum(r.rows_inserted for r in self.results),
            "rows_duplicate": sum(r.rows_duplicate for r in self.results),
            "results": [r.to_json_dict() for r in self.results],
        }

    def write(self, control_store: ObjectStore) -> str:
        key = f"reports/backfill/{self.run_id}.json"
        control_store.write_object(key, (json.dumps(self.to_json_dict(), indent=2) + "\n").encode())
        return key


def load_id_for(batch_id: str) -> str:
    return f"load-{batch_id}"


class WarehouseLoader:
    def __init__(
        self,
        client: WarehouseClient,
        store: ObjectStore,
        source: str,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.client = client
        self.store = store
        self.source = source
        self.clock = clock

    # --- setup / discovery ------------------------------------------------------

    def setup(self) -> None:
        self.client.ensure_table(RAW_EVENTS)
        self.client.ensure_table(LOAD_AUDIT)

    def committed_manifests(self) -> list[BatchManifest]:
        manifests = []
        for batch_id in list_batch_ids(self.store, self.source):
            manifests.append(
                BatchManifest.from_bytes(
                    self.store.read_object(manifest_key(self.source, batch_id))
                )
            )
        return sorted(manifests, key=lambda m: (m.min_event_time or m.ingested_at, m.batch_id))

    def latest_status(self, batch_id: str) -> LoadStatus | None:
        rows = self.client.query(
            f"SELECT status FROM {self.client.ref(LOAD_AUDIT.name)} "
            f"WHERE batch_id = {_literal(batch_id)} ORDER BY attempt DESC LIMIT 1"
        )
        return LoadStatus(rows[0]["status"]) if rows else None

    def _next_attempt(self, batch_id: str) -> int:
        rows = self.client.query(
            f"SELECT COALESCE(MAX(attempt), 0) AS n FROM {self.client.ref(LOAD_AUDIT.name)} "
            f"WHERE batch_id = {_literal(batch_id)}"
        )
        return int(rows[0]["n"]) + 1

    # --- loading ---------------------------------------------------------------------

    def _rows(
        self, manifest: BatchManifest, load_id: str, loaded_at: datetime
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for entry in manifest.objects:
            data = self.store.read_object(entry.key)
            if sha256_hex(data) != entry.sha256:
                raise ValueError(f"source checksum mismatch: {entry.key}")
            for line in data.decode().splitlines():
                if not line.strip():
                    continue
                record = json.loads(line)
                event, ingest = record["event"], record["ingest"]
                event_time = datetime.fromisoformat(event["event_time"])
                rows.append(
                    {
                        "event_id": event["event_id"],
                        "event_type": event["event_type"],
                        "schema_version": event["schema_version"],
                        "event_time": event_time,
                        "event_date": event_time.date(),
                        "producer": event.get("producer"),
                        "correlation_id": event.get("correlation_id"),
                        "payload": json.dumps(
                            event["payload"], sort_keys=True, separators=(",", ":")
                        ),
                        "ingested_at": datetime.fromisoformat(ingest["ingested_at"]),
                        "is_late": ingest.get("is_late"),
                        "source_topic": ingest.get("source_topic"),
                        "kafka_partition": ingest.get("kafka_partition"),
                        "kafka_offset": ingest.get("kafka_offset"),
                        "run_id": ingest.get("run_id"),
                        "batch_id": manifest.batch_id,
                        "load_id": load_id,
                        "loaded_at": loaded_at,
                    }
                )
        return rows

    def load_batch(self, manifest: BatchManifest) -> LoadResult:
        load_id = load_id_for(manifest.batch_id)
        started = self.clock()
        staging = f"_stg_raw_events_{sanitize_for_identifier(manifest.batch_id)}"
        rows_read = inserted = duplicate = 0
        error: str | None = None
        try:
            self.setup()
            rows = self._rows(manifest, load_id, started)
            rows_read = len(rows)
            if rows_read != manifest.event_count:
                raise ValueError(
                    f"row count {rows_read} != manifest event_count {manifest.event_count}"
                )
            if len({r["event_id"] for r in rows}) != rows_read:
                raise ValueError("batch contains duplicate event_ids")

            lo = min(r["event_date"] for r in rows).isoformat()
            hi = max(r["event_date"] for r in rows).isoformat()
            self.client.drop_table(staging)
            stage_spec = RAW_EVENTS.with_name(staging)
            self.client.ensure_table(stage_spec)
            self.client.load_rows(stage_spec, rows)

            target, stg = self.client.ref(RAW_EVENTS.name), self.client.ref(staging)
            window = f"t.event_date BETWEEN DATE '{lo}' AND DATE '{hi}'"
            duplicate = int(
                self.client.query(
                    f"SELECT COUNT(*) AS n FROM {stg} AS s WHERE EXISTS "
                    f"(SELECT 1 FROM {target} AS t WHERE {window} AND t.event_id = s.event_id)"
                )[0]["n"]
            )
            cols = RAW_EVENTS.column_names
            self.client.execute(
                f"MERGE INTO {target} AS t USING {stg} AS s "
                f"ON {window} AND t.event_id = s.event_id "
                f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
                f"VALUES ({', '.join('s.' + c for c in cols)})"
            )
            present = int(
                self.client.query(
                    f"SELECT COUNT(*) AS n FROM {stg} AS s WHERE EXISTS "
                    f"(SELECT 1 FROM {target} AS t WHERE {window} AND t.event_id = s.event_id)"
                )[0]["n"]
            )
            if present != rows_read:
                raise WarehouseError(
                    f"post-merge check failed: {present} of {rows_read} rows present"
                )
            inserted = rows_read - duplicate
        except (StorageError, WarehouseError, ValueError, KeyError) as exc:
            error = f"{type(exc).__name__}: {exc}"[:500]
            inserted = duplicate = 0
        finally:
            with contextlib.suppress(WarehouseError):  # scratch table; also auto-expires in BQ
                self.client.drop_table(staging)

        status = LoadStatus.FAILED if error else LoadStatus.SUCCEEDED
        result = LoadResult(
            manifest.batch_id,
            load_id,
            status,
            manifest.event_count,
            rows_read,
            inserted,
            duplicate,
            error,
        )
        self.client.load_rows(
            LOAD_AUDIT,
            [
                {
                    "load_id": load_id,
                    "batch_id": manifest.batch_id,
                    "attempt": self._next_attempt(manifest.batch_id),
                    "source": self.source,
                    "status": status.value,
                    "expected_rows": manifest.event_count,
                    "rows_read": rows_read,
                    "rows_inserted": inserted,
                    "rows_duplicate": duplicate,
                    "started_at": started,
                    "finished_at": self.clock(),
                    "error": error,
                }
            ],
        )
        return result

    def backfill(
        self, start: date, end: date, run_id: str, *, skip_succeeded: bool = False
    ) -> BackfillReport:
        """Load every committed batch overlapping ``[start, end]`` (inclusive).

        Whole batches are loaded; events outside the range inside a selected
        batch are loaded too (idempotent). Safe to run repeatedly.
        """
        if end < start:
            raise ValueError("end must not be before start")
        self.setup()
        report = BackfillReport(run_id, start, end)
        for manifest in self.committed_manifests():
            if manifest.min_event_time is None or manifest.max_event_time is None:
                continue
            if manifest.min_event_time.date() > end or manifest.max_event_time.date() < start:
                continue
            if skip_succeeded and self.latest_status(manifest.batch_id) is LoadStatus.SUCCEEDED:
                report.skipped.append(manifest.batch_id)
                continue
            report.results.append(self.load_batch(manifest))
        return report


__all__ = ["BackfillReport", "LoadResult", "LoadStatus", "WarehouseLoader", "load_id_for"]
