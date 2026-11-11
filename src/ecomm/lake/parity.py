"""Cross-cloud parity reconciliation.

For each batch, every destination is checked independently against its own
manifest (objects exist, bytes match the recorded SHA-256, record counts match)
and then destinations are compared with each other on the *logical* level
(event count and the checksum of the set of event IDs).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from ecomm.lake.manifest import (
    BatchManifest,
    logical_checksum,
    manifest_key,
    manifest_prefix,
    sha256_hex,
)
from ecomm.lake.raw import event_ids_in_object
from ecomm.storage.interface import ObjectNotFoundError, ObjectStore, StorageError


class ParityStatus(StrEnum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"


@dataclass(frozen=True)
class DestinationCheck:
    manifest_found: bool
    event_count: int = 0
    logical_checksum: str = ""
    problems: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.manifest_found and not self.problems


@dataclass(frozen=True)
class ParityReport:
    batch_id: str
    status: ParityStatus
    destinations: dict[str, DestinationCheck] = field(default_factory=dict)
    problems: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.status is ParityStatus.MATCH


def _check_destination(store: ObjectStore, source: str, batch_id: str) -> DestinationCheck:
    try:
        manifest = BatchManifest.from_bytes(store.read_object(manifest_key(source, batch_id)))
    except ObjectNotFoundError:
        return DestinationCheck(False, problems=("manifest missing",))
    except (ValueError, StorageError) as exc:
        return DestinationCheck(False, problems=(f"manifest unreadable: {type(exc).__name__}",))

    problems: list[str] = []
    ids: list[str] = []
    for entry in manifest.objects:
        try:
            data = store.read_object(entry.key)
        except ObjectNotFoundError:
            problems.append(f"object missing: {entry.key}")
            continue
        except StorageError as exc:
            problems.append(f"object unreadable: {entry.key} ({type(exc).__name__})")
            continue
        if sha256_hex(data) != entry.sha256:
            problems.append(f"checksum mismatch: {entry.key}")
        found = event_ids_in_object(data)
        if len(found) != entry.record_count:
            problems.append(f"record count mismatch: {entry.key}")
        ids.extend(found)

    actual = logical_checksum(ids)
    if len(ids) != manifest.event_count:
        problems.append(f"event count {len(ids)} != manifest {manifest.event_count}")
    if actual != manifest.logical_checksum:
        problems.append("logical checksum differs from manifest")
    return DestinationCheck(True, len(ids), actual, tuple(problems))


def reconcile_batch(stores: Mapping[str, ObjectStore], source: str, batch_id: str) -> ParityReport:
    checks = {name: _check_destination(store, source, batch_id) for name, store in stores.items()}
    problems = [f"{name}: {p}" for name, c in checks.items() for p in c.problems]
    present = [c for c in checks.values() if c.manifest_found]
    if len({(c.event_count, c.logical_checksum) for c in present}) > 1:
        problems.append("destinations disagree on event count or logical checksum")
    status = ParityStatus.MATCH if not problems else ParityStatus.MISMATCH
    return ParityReport(batch_id, status, checks, tuple(problems))


def list_batch_ids(store: ObjectStore, source: str) -> list[str]:
    prefix = manifest_prefix(source)
    ids = set()
    for key in store.list_objects(prefix):
        part = key.removeprefix(prefix).split("/", 1)[0]
        if part.startswith("batch_id="):
            ids.add(part.removeprefix("batch_id="))
    return sorted(ids)


def reconcile_all(stores: Mapping[str, ObjectStore], source: str) -> list[ParityReport]:
    """Reconcile every batch known to *any* destination."""
    batch_ids = sorted({b for store in stores.values() for b in list_batch_ids(store, source)})
    return [reconcile_batch(stores, source, b) for b in batch_ids]


# --- light check: manifests and object presence only (cheap enough for a metrics endpoint) -------


def reconcile_all_light(stores: Mapping[str, ObjectStore], source: str) -> list[ParityReport]:
    """Compare destinations without reading or hashing any data object.

    For every batch known to any destination: the manifest must exist in each, the manifests must
    describe the same content (event count, logical checksum, object keys and SHA-256s), and every
    object a manifest lists must exist (one directory listing per destination, no object reads).

    This catches a missing or divergent commit, which is what a *monitor* needs on every refresh.
    It does **not** detect an object whose bytes were altered in place; that needs the full
    :func:`reconcile_all` (the daily reconciliation DAG, and the one-shot exporter).
    """
    listings = {name: set(store.list_objects("raw/")) for name, store in stores.items()}
    batch_ids = sorted({b for store in stores.values() for b in list_batch_ids(store, source)})
    return [_light_report(stores, listings, source, b) for b in batch_ids]


def _light_report(
    stores: Mapping[str, ObjectStore], listings: Mapping[str, set[str]], source: str, batch_id: str
) -> ParityReport:
    checks: dict[str, DestinationCheck] = {}
    fingerprints: dict[str, tuple[int, str, tuple[tuple[str, str], ...]]] = {}
    for name, store in stores.items():
        try:
            manifest = BatchManifest.from_bytes(store.read_object(manifest_key(source, batch_id)))
        except ObjectNotFoundError:
            checks[name] = DestinationCheck(False, problems=("manifest missing",))
            continue
        except (ValueError, StorageError) as exc:
            problem = f"manifest unreadable: {type(exc).__name__}"
            checks[name] = DestinationCheck(False, problems=(problem,))
            continue
        missing = [e.key for e in manifest.objects if e.key not in listings[name]]
        checks[name] = DestinationCheck(
            True,
            manifest.event_count,
            manifest.logical_checksum,
            tuple(f"object missing: {key}" for key in missing[:5]),
        )
        fingerprints[name] = (
            manifest.event_count,
            manifest.logical_checksum,
            tuple(sorted((e.key, e.sha256) for e in manifest.objects)),
        )
    problems = [f"{name}: {p}" for name, c in checks.items() for p in c.problems]
    if len(set(fingerprints.values())) > 1:
        problems.append("destinations hold different manifests for this batch")
    status = ParityStatus.MATCH if not problems else ParityStatus.MISMATCH
    return ParityReport(batch_id, status, checks, tuple(problems))
