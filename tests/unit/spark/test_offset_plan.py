"""Completeness check against Spark's planned offsets (regression for silent watermark drops).

The checkpoint text below is copied from a real run in which Spark discarded 15 ``customers``
messages (older than its watermark) before they reached the sink: batch 3 planned them, the sink
received none, and nothing complained. These tests pin that failure and its detection.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ecomm.spark.rows import STATUS_INVALID
from ecomm.spark.sink import (
    BatchRow,
    OffsetGapError,
    OffsetPlan,
    SinkConfig,
    load_offset_plan,
    parse_offsets_log,
    process_batch,
    verify_against_plan,
)
from ecomm.storage.testing import InMemoryObjectStore

REAL_BATCH_2 = (
    'v1\n{"batchWatermarkMs":1793565059794,"batchTimestampMs":1794332944479,'
    '"conf":{"spark.sql.shuffle.partitions":"2"}}\n'
    '{"commerce.payments":{"2":96,"1":125,"0":97},"commerce.orders":{"2":149,"1":148,"0":177},'
    '"commerce.inventory":{"2":129,"1":92,"0":126},"commerce.customers":{"2":9,"1":15,"0":18}} '
)
REAL_BATCH_3 = (
    'v1\n{"batchWatermarkMs":1793617384348,"batchTimestampMs":1794333175471,'
    '"conf":{"spark.sql.shuffle.partitions":"2"}}\n'
    '{"commerce.payments":{"2":121,"1":157,"0":122},"commerce.orders":{"2":187,"1":186,"0":222},'
    '"commerce.inventory":{"2":163,"1":116,"0":159},"commerce.customers":{"2":13,"1":20,"0":24}}'
)


def row(topic: str, partition: int, offset: int) -> BatchRow:
    return BatchRow(
        topic, partition, offset, "k", b"{x", 1_000 + offset, STATUS_INVALID, None, None, None,
        False, "INVALID_JSON", "x", False,
    )  # fmt: skip


def rows_for(plan: OffsetPlan, skip: set[tuple[str, int]] | None = None) -> list[BatchRow]:
    """Every row the plan promises, minus the (topic/partition, offset) pairs in ``skip``."""
    skip = skip or set()
    out = []
    for tp, end in plan.end.items():
        topic, partition = tp.rsplit("/", 1)
        start = plan.start[tp] if plan.start else 0
        out.extend(row(topic, int(partition), o) for o in range(start, end) if (tp, o) not in skip)
    return sorted(out, key=lambda r: (r.topic, r.partition, r.offset))


# --- parsing the real checkpoint format ----------------------------------------------------------


def test_parse_real_offsets_log() -> None:
    parsed = parse_offsets_log(REAL_BATCH_3)
    assert parsed["commerce.customers/0"] == 24 and parsed["commerce.orders/2"] == 187
    assert len(parsed) == 12  # 4 topics x 3 partitions
    assert parse_offsets_log(REAL_BATCH_2)["commerce.customers/2"] == 9  # trailing space tolerated


@pytest.mark.parametrize("text", ["", "v1\n{}", "garbage\n{}\n{}", "v1\n{}\nnot json"])
def test_parse_rejects_unrecognised_files(text: str) -> None:
    with pytest.raises(ValueError):
        parse_offsets_log(text)


def test_load_offset_plan_reads_this_and_the_previous_batch(tmp_path: Path) -> None:
    offsets = tmp_path / "offsets"
    offsets.mkdir()
    (offsets / "2").write_text(REAL_BATCH_2)
    (offsets / "3").write_text(REAL_BATCH_3)
    plan = load_offset_plan(tmp_path, 3)
    assert plan.end["commerce.customers/0"] == 24 and plan.start is not None
    assert plan.start["commerce.customers/0"] == 18
    first = load_offset_plan(tmp_path, 2)  # no batch 1 file present: start unknown
    assert first.start is None
    with pytest.raises(FileNotFoundError):
        load_offset_plan(tmp_path, 9)


# --- verification ------------------------------------------------------------------------------


def real_plan() -> OffsetPlan:
    return OffsetPlan(parse_offsets_log(REAL_BATCH_3), parse_offsets_log(REAL_BATCH_2))


def test_a_complete_batch_passes() -> None:
    plan = real_plan()
    assert verify_against_plan(rows_for(plan), plan) == []


def test_the_real_failure_all_customers_rows_missing_is_detected() -> None:
    """Exactly what happened: the planned customers messages never reached the sink."""
    plan = real_plan()
    received = [r for r in rows_for(plan) if r.topic != "commerce.customers"]
    problems = verify_against_plan(received, plan)
    assert len(problems) == 3  # one per customers partition
    assert all("commerce.customers/" in p and "received 0" in p for p in problems)
    assert "planned offsets 18..23 (6 messages)" in next(p for p in problems if "customers/0" in p)


def test_missing_tail_head_and_middle_rows_are_all_detected() -> None:
    plan = real_plan()
    tail = verify_against_plan(rows_for(plan, {("commerce.orders/0", 221)}), plan)
    head = verify_against_plan(rows_for(plan, {("commerce.orders/0", 177)}), plan)
    middle = verify_against_plan(rows_for(plan, {("commerce.orders/0", 200)}), plan)
    for problems, missing in ((tail, 221), (head, 177), (middle, 200)):
        assert len(problems) == 1 and f"first missing offset {missing}" in problems[0]


def test_unplanned_and_duplicate_rows_are_flagged() -> None:
    plan = real_plan()
    extra = [*rows_for(plan), row("commerce.orders", 7, 0)]
    assert any("planned none" in p for p in verify_against_plan(extra, plan))
    doubled = sorted(
        [*rows_for(plan), row("commerce.orders", 0, 200)],
        key=lambda r: (r.topic, r.partition, r.offset),
    )
    assert verify_against_plan(doubled, plan)


def test_partitions_with_nothing_new_expect_no_rows() -> None:
    plan = OffsetPlan(end={"t/0": 5, "t/1": 9}, start={"t/0": 5, "t/1": 6})
    assert verify_against_plan([row("t", 1, 6), row("t", 1, 7), row("t", 1, 8)], plan) == []
    assert verify_against_plan([], OffsetPlan(end={"t/0": 5}, start={"t/0": 5})) == []


def test_first_batch_without_a_previous_plan_uses_the_first_row_as_start() -> None:
    plan = OffsetPlan(end={"t/0": 4})
    assert verify_against_plan([row("t", 0, 1), row("t", 0, 2), row("t", 0, 3)], plan) == []
    assert verify_against_plan([row("t", 0, 1), row("t", 0, 3)], plan)  # interior hole still caught
    assert verify_against_plan([row("t", 0, 1), row("t", 0, 2)], plan)  # tail drop still caught


# --- the sink refuses the batch, writes nothing, and says what to do ------------------------------------


def sink() -> tuple[SinkConfig, InMemoryObjectStore, InMemoryObjectStore, InMemoryObjectStore]:
    s3, gcs, control = (
        InMemoryObjectStore("s3"),
        InMemoryObjectStore("gcs"),
        InMemoryObjectStore("c"),
    )
    return (
        SinkConfig("commerce", "t", {"s3": s3, "gcs": gcs}, control, sleep=lambda _: None),
        s3,
        gcs,
        control,
    )


def test_sink_fails_loudly_before_writing_when_planned_rows_never_arrived() -> None:
    cfg, s3, gcs, control = sink()
    plan = real_plan()
    received = [r for r in rows_for(plan) if r.topic != "commerce.customers"]
    with pytest.raises(OffsetGapError, match="--dedup-retention-hours"):
        process_batch(received, 3, cfg, plan)
    assert not s3.objects and not gcs.objects and not control.objects


def test_sink_fails_even_when_a_batch_delivered_zero_rows() -> None:
    cfg, s3, *_ = sink()
    plan = OffsetPlan(end={"commerce.orders/0": 10}, start={"commerce.orders/0": 5})
    with pytest.raises(OffsetGapError):
        process_batch([], 3, cfg, plan)  # every planned message was dropped
    assert not s3.objects


def test_sink_accepts_a_complete_batch_and_the_check_can_be_disabled() -> None:
    cfg, s3, *_ = sink()
    plan = OffsetPlan(end={"commerce.orders/0": 3}, start={"commerce.orders/0": 0})
    assert process_batch(rows_for(plan), 0, cfg, plan).quarantined == 3
    cfg.fail_on_offset_gap = False
    lossy = rows_for(plan, {("commerce.orders/0", 2)})
    assert process_batch(lossy, 1, cfg, plan).input_rows == 2  # opt-out: not verified
