"""Serialisation of generator results (fixture files and stream records)."""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

from ecomm.contracts.types import format_utc
from ecomm.generator import Emission, GenerationResult


def emission_record(emission: Emission) -> dict[str, Any]:
    """What a producer sends. ``value`` is a string because it may be invalid JSON."""
    return {
        "seq": emission.seq,
        "arrival_time": format_utc(emission.arrival_time),
        "topic": emission.topic,
        "key": emission.key,
        "value": emission.value,
    }


def label_record(emission: Emission) -> dict[str, Any]:
    """Ground truth for tests and demos; never sent through the pipeline."""
    return {
        "seq": emission.seq,
        "event_id": emission.event_id,
        "event_type": emission.event_type,
        "event_time": format_utc(emission.event_time),
        "fault": emission.fault.value if emission.fault else None,
        "fault_detail": emission.fault_detail,
    }


def summarise(result: GenerationResult) -> dict[str, Any]:
    emissions = result.emissions
    return {
        "total_emissions": len(emissions),
        "distinct_event_ids": len({e.event_id for e in emissions}),
        "by_event_type": dict(sorted(Counter(e.event_type for e in emissions).items())),
        "by_topic": dict(sorted(Counter(e.topic for e in emissions).items())),
        "by_fault": dict(sorted(Counter(e.fault.value for e in emissions if e.fault).items())),
        "products": len(result.products),
    }


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


PRODUCT_COLUMNS = (
    "product_id",
    "sku",
    "product_name",
    "category",
    "unit_price",
    "currency",
    "is_active",
)


def write_products_csv(result: GenerationResult, path: Path) -> None:
    """Product catalogue as a dbt seed (``dbt/seeds/products.csv``)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(PRODUCT_COLUMNS)
        for product in result.products:
            row = product.to_json_dict()
            writer.writerow(
                [
                    str(row[c]).lower() if isinstance(row[c], bool) else row[c]
                    for c in PRODUCT_COLUMNS
                ]
            )


def write_fixture(result: GenerationResult, directory: Path) -> dict[str, Any]:
    """Write events, labels and products (JSONL + CSV seed) and ``summary.json``."""
    directory.mkdir(parents=True, exist_ok=True)
    _write_jsonl(directory / "events.jsonl", [emission_record(e) for e in result.emissions])
    _write_jsonl(directory / "labels.jsonl", [label_record(e) for e in result.emissions])
    _write_jsonl(directory / "products.jsonl", [p.to_json_dict() for p in result.products])
    write_products_csv(result, directory / "products.csv")
    summary = summarise(result)
    (directory / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    return summary
