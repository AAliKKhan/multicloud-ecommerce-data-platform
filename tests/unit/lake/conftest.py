from __future__ import annotations

import pytest
from lake_helpers import CONFIG, LATE_THRESHOLD, PROCESSED_AT

from ecomm.generator import FaultKind, GenerationResult, generate
from ecomm.lake.ingest import ClassifiedBatch, SourceMessage, classify
from ecomm.lake.raw import PreparedBatch, prepare_batch
from ecomm.lake.sources import messages_from_emissions


@pytest.fixture(scope="session")
def generated() -> GenerationResult:
    return generate(CONFIG)


@pytest.fixture(scope="session")
def messages(generated: GenerationResult) -> list[SourceMessage]:
    return messages_from_emissions(generated.emissions)


@pytest.fixture(scope="session")
def classified(messages: list[SourceMessage]) -> ClassifiedBatch:
    return classify(messages, set(), processed_at=PROCESSED_AT, late_threshold=LATE_THRESHOLD)


@pytest.fixture(scope="session")
def fault_counts(generated: GenerationResult) -> dict[FaultKind, int]:
    counts: dict[FaultKind, int] = {}
    for e in generated.emissions:
        if e.fault:
            counts[e.fault] = counts.get(e.fault, 0) + 1
    return counts


@pytest.fixture()
def prepared(classified: ClassifiedBatch) -> PreparedBatch:
    return prepare_batch(
        classified.valid,
        source="commerce",
        run_id="run-001",
        batch_id="run-001-b00000",
        ingested_at=PROCESSED_AT,
    )
