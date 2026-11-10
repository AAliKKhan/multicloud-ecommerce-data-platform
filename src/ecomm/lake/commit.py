"""Dual-destination batch commit with bounded retry and a manifest commit marker.

Protocol per destination (each destination is independent):

1. If a manifest identical to ours already exists -> already committed, skip
   (this is what makes re-running a batch idempotent).
2. Write every data object, then read each one back and compare its SHA-256.
3. Write the manifest *last*. A destination is only ``COMMITTED`` once its
   manifest exists; readers must ignore objects without a manifest.

A batch is ``COMPLETE`` only when every *required* destination is committed.
If some are, it is ``PARTIAL``: the successful destination is left intact and
re-running :func:`commit_batch` with the same batch retries only the failed
one, using the same deterministic keys. This is dual-write replication with
reconciliation, **not** automatic failover (ADR 0001).
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from ecomm.lake.manifest import BatchManifest, manifest_key, sha256_hex
from ecomm.lake.raw import PreparedBatch
from ecomm.storage.interface import ObjectNotFoundError, ObjectStore, StorageError


class DestinationState(StrEnum):
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


class BatchStatus(StrEnum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 4
    base_delay_s: float = 0.5
    max_delay_s: float = 8.0

    def delay(self, attempt: int) -> float:
        """Delay before retry number ``attempt`` (1-based), exponential and capped."""
        return min(self.max_delay_s, self.base_delay_s * 2.0 ** (attempt - 1))


@dataclass(frozen=True)
class DestinationResult:
    state: DestinationState
    attempts: int
    already_committed: bool = False
    error: str | None = None


@dataclass(frozen=True)
class BatchResult:
    batch_id: str
    source: str
    status: BatchStatus
    destinations: dict[str, DestinationResult] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, object]:
        return {
            "batch_id": self.batch_id,
            "source": self.source,
            "status": self.status.value,
            "destinations": {
                name: {
                    "state": r.state.value,
                    "attempts": r.attempts,
                    "already_committed": r.already_committed,
                    "error": r.error,
                }
                for name, r in sorted(self.destinations.items())
            },
        }


def _already_committed(store: ObjectStore, batch: PreparedBatch) -> bool:
    key = manifest_key(batch.manifest.source, batch.manifest.batch_id)
    try:
        existing = BatchManifest.from_bytes(store.read_object(key))
    except ObjectNotFoundError:
        return False
    except ValueError:
        return False  # corrupt manifest: rewrite it
    return existing == batch.manifest


def _write_destination(store: ObjectStore, batch: PreparedBatch) -> None:
    manifest = batch.manifest
    meta = {"batch_id": manifest.batch_id, "run_id": manifest.run_id}
    for key, data in batch.objects.items():
        store.write_object(key, data, {**meta, "sha256": sha256_hex(data)})
    for entry in manifest.objects:
        if sha256_hex(store.read_object(entry.key)) != entry.sha256:
            raise StorageError(f"read-back checksum mismatch for {entry.key}")
    store.write_object(manifest_key(manifest.source, manifest.batch_id), manifest.to_bytes(), meta)


def commit_batch(
    batch: PreparedBatch,
    stores: Mapping[str, ObjectStore],
    required: Sequence[str] | None = None,
    retry: RetryPolicy | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> BatchResult:
    """Write ``batch`` to every store; safe to call again for the same batch."""
    retry = retry or RetryPolicy()
    required_names = list(required) if required is not None else list(stores)
    unknown = set(required_names) - set(stores)
    if unknown:
        raise ValueError(f"required destinations not configured: {sorted(unknown)}")

    results: dict[str, DestinationResult] = {}
    for name, store in stores.items():
        if _already_committed(store, batch):
            results[name] = DestinationResult(DestinationState.COMMITTED, 0, already_committed=True)
            continue
        error: str | None = None
        for attempt in range(1, retry.max_attempts + 1):
            try:
                _write_destination(store, batch)
            except StorageError as exc:
                error = str(exc)
                if attempt < retry.max_attempts:
                    sleep(retry.delay(attempt))
                continue
            results[name] = DestinationResult(DestinationState.COMMITTED, attempt)
            break
        else:
            results[name] = DestinationResult(
                DestinationState.FAILED, retry.max_attempts, error=error
            )

    committed = [n for n in required_names if results[n].state is DestinationState.COMMITTED]
    if len(committed) == len(required_names):
        status = BatchStatus.COMPLETE
    elif any(r.state is DestinationState.COMMITTED for r in results.values()):
        status = BatchStatus.PARTIAL
    else:
        status = BatchStatus.FAILED
    return BatchResult(batch.manifest.batch_id, batch.manifest.source, status, results)


def ledger_key(source: str, batch_id: str) -> str:
    return f"ledger/source={source}/batch_id={batch_id}.json"


def record_status(control_store: ObjectStore, result: BatchResult) -> None:
    """Persist the latest batch status (overwritten on each retry)."""
    payload = json.dumps(result.to_json_dict(), indent=2, sort_keys=True) + "\n"
    control_store.write_object(ledger_key(result.source, result.batch_id), payload.encode())
