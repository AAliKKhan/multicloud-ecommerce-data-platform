"""Stateful de-duplication step (no Spark imports).

Spark calls this once per ``event_id`` per micro-batch through
``applyInPandasWithState``; the state (last-seen time) lives in the checkpoint,
so a restarted query keeps recognising duplicates seen before the restart.

State is bounded: it expires ``retention_ms`` after an event ID was last seen.
A duplicate that arrives after that horizon is *not* caught here; the
idempotent warehouse ``MERGE`` on ``event_id`` still removes it (ADR 0001).
"""

from __future__ import annotations

from typing import Any

from ecomm.spark.rows import STATUS_OK

Row = dict[str, Any]


def dedup_step(
    rows: list[Row], state_last_seen_ms: int | None, now_ms: int
) -> tuple[list[Row], int | None]:
    """Mark each row ``is_duplicate`` and return the new state.

    ``rows`` are the rows of one key in this micro-batch. They are processed in
    ``(partition, offset)`` order so the first copy seen is deterministic. The
    first valid row of a never-seen key is new; every later valid row is a
    duplicate. Invalid rows are never duplicates and leave the state untouched.
    """
    ordered = sorted(rows, key=lambda r: (r["partition"], r["offset"]))
    seen = state_last_seen_ms is not None
    out: list[Row] = []
    touched = False
    for row in ordered:
        if row["status"] != STATUS_OK:
            out.append({**row, "is_duplicate": False})
            continue
        out.append({**row, "is_duplicate": seen})
        seen = True
        touched = True
    new_state = now_ms if touched else state_last_seen_ms
    return out, new_state
