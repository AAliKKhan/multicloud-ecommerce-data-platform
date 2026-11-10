"""Per-run ingest statistics (valid / duplicate / quarantined / late counts).

Written to the control store so observability and quality checks can report
duplicate and quarantine rates, which are otherwise only visible while a job runs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from ecomm.contracts.types import format_utc
from ecomm.lake.ingest import ClassifiedBatch
from ecomm.storage.interface import ObjectStore


@dataclass(frozen=True)
class IngestStats:
    run_id: str
    processed_at: datetime
    input_count: int
    valid: int
    duplicates: int
    quarantined: int
    late: int

    @classmethod
    def from_classified(
        cls, run_id: str, processed_at: datetime, batch: ClassifiedBatch
    ) -> IngestStats:
        return cls(
            run_id=run_id,
            processed_at=processed_at,
            input_count=batch.input_count,
            valid=len(batch.valid),
            duplicates=len(batch.duplicates),
            quarantined=len(batch.quarantined),
            late=batch.late_count,
        )

    def to_json_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "processed_at": format_utc(self.processed_at),
            "input_count": self.input_count,
            "valid": self.valid,
            "duplicates": self.duplicates,
            "quarantined": self.quarantined,
            "late": self.late,
        }

    @classmethod
    def from_json_dict(cls, data: dict[str, object]) -> IngestStats:
        return cls(
            run_id=str(data["run_id"]),
            processed_at=datetime.fromisoformat(str(data["processed_at"])),
            input_count=int(str(data["input_count"])),
            valid=int(str(data["valid"])),
            duplicates=int(str(data["duplicates"])),
            quarantined=int(str(data["quarantined"])),
            late=int(str(data["late"])),
        )


def stats_prefix(source: str) -> str:
    return f"ingest/source={source}/"


def record_ingest_stats(control_store: ObjectStore, source: str, stats: IngestStats) -> str:
    key = f"{stats_prefix(source)}run_id={stats.run_id}.json"
    payload = json.dumps(stats.to_json_dict(), indent=2, sort_keys=True) + "\n"
    control_store.write_object(key, payload.encode())
    return key


def list_ingest_stats(control_store: ObjectStore, source: str) -> list[IngestStats]:
    return [
        IngestStats.from_json_dict(json.loads(control_store.read_object(key)))
        for key in control_store.list_objects(stats_prefix(source))
    ]
