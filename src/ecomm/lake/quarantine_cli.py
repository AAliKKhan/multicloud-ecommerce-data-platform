"""``ecomm-quarantine``: inspect events rejected by ingest.

    uv run ecomm-quarantine --demo-dir data/demo              # summary + first 5 samples
    uv run ecomm-quarantine --demo-dir data/demo --code INVALID_JSON --show 20
    uv run ecomm-quarantine --demo-dir data/demo --json       # machine-readable

Quarantined records keep the error code, error detail, original payload,
source topic/partition/offset and processing time, so a rejected event can be
diagnosed and, after a fix, replayed from the raw payload. Payloads are
truncated on display; the data is synthetic.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from ecomm.lake.stores import lake_layout, open_lake
from ecomm.storage.interface import ObjectStore, StorageConfigError

HINTS = {
    "INVALID_JSON": "Truncated or non-JSON message. Check the producer; payload is preserved for replay.",
    "NOT_AN_OBJECT": "Message is JSON but not an object. Check the producer serializer.",
    "MISSING_FIELD": "A required envelope field is absent. Check the producer's event builder.",
    "INVALID_FIELD": "An envelope field has the wrong type/format (e.g. timestamp, UUID).",
    "UNKNOWN_EVENT_TYPE": "Unrecognised event_type. Possibly a newer producer; add the type or version.",
    "UNSUPPORTED_SCHEMA_VERSION": "Newer schema_version. Add parsing for it, then replay these events.",
    "INVALID_PAYLOAD": "Payload violates the event contract. Compare with docs/data-dictionary.md.",
}


def load_quarantine(store: ObjectStore, source: str = "commerce") -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for key in store.list_objects(f"quarantine/source={source}/"):
        for line in store.read_object(key).decode().splitlines():
            if line.strip():
                records.append(json.loads(line))
    return records


def summarise(records: list[dict[str, Any]]) -> dict[str, Any]:
    times = sorted(str(r["processed_at"]) for r in records)
    return {
        "total": len(records),
        "by_error_code": dict(Counter(r["error_code"] for r in records).most_common()),
        "by_topic": dict(Counter(r["topic"] for r in records).most_common()),
        "first_processed_at": times[0] if times else None,
        "last_processed_at": times[-1] if times else None,
    }


def format_report(records: list[dict[str, Any]], show: int, max_chars: int = 100) -> str:
    summary = summarise(records)
    lines = [f"Quarantined messages: {summary['total']}"]
    if not records:
        return lines[0] + "\nNothing to inspect."
    lines.append("By error code:")
    for code, count in summary["by_error_code"].items():
        lines.append(f"  {count:>5}  {code}  - {HINTS.get(code, 'See docs/operations.md')}")
    lines.append("By topic: " + ", ".join(f"{t}={n}" for t, n in summary["by_topic"].items()))
    for record in records[:show]:
        payload = str(record["original_payload"])
        shown = payload if len(payload) <= max_chars else payload[:max_chars] + "..."
        lines.append(
            f"- [{record['error_code']}] {record['topic']}/{record['partition']}@{record['offset']}"
            f" detail={record['error_detail']!r}\n    payload: {shown}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ecomm-quarantine", description="Inspect quarantined events."
    )
    parser.add_argument("--demo-dir", type=Path, default=Path("data/demo"))
    parser.add_argument("--source", default="commerce")
    parser.add_argument("--code", help="only show this error code")
    parser.add_argument("--show", type=int, default=5, help="number of sample records")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    try:
        layout = lake_layout(args.demo_dir)
        if layout.s3_directory is not None and not layout.s3_directory.is_dir():
            print(f"error: {layout.s3_directory} not found; run 'uv run ecomm-demo' first")
            return 2
        lake = open_lake(args.demo_dir)  # opening creates local directories: check first
    except StorageConfigError as exc:
        print(f"error: {exc}")
        return 2
    records = load_quarantine(lake.s3, args.source)
    if args.code:
        records = [r for r in records if r["error_code"] == args.code]

    if args.json:
        print(
            json.dumps({"summary": summarise(records), "samples": records[: args.show]}, indent=2)
        )
    else:
        print(format_report(records, args.show))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
