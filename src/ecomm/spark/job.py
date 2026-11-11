"""Spark Structured Streaming job: Kafka -> validate -> stateful de-dup -> raw landing.

    docker compose --profile spark run --rm spark                      # process everything, then exit
    docker compose --profile spark run --rm spark --trigger processing-time:10s   # keep running

Pipeline
--------
1. ``readStream`` from the ``<prefix>.{orders,payments,inventory,customers}`` topics.
2. A UDF validates every message with the shared contract validator
   (:func:`ecomm.spark.rows.classify_value`) and flags late events.
3. ``applyInPandasWithState`` de-duplicates on ``event_id``. Its state lives in the
   checkpoint, so duplicates seen before a restart are still recognised; state
   expires after ``--dedup-retention-hours`` (later duplicates are caught by the
   warehouse ``MERGE``).
4. ``foreachBatch`` hands each micro-batch to :func:`ecomm.spark.sink.process_batch`,
   which writes partitioned raw JSONL + manifests to both destinations, quarantines
   invalid messages and records statistics. The batch ID comes from Spark's micro-batch
   number, so a replay after a crash rewrites identical data (idempotent).

Local substitutes: the two destinations are directories (``s3-local`` and ``gcs-local``)
under ``--lake-dir``; they stand in for S3 and GCS and use the same layout as the demo,
so the loader, quality checks and metrics work on this output unchanged.

PySpark is imported lazily so this module (arguments, config) is importable without it.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, timedelta
from pathlib import Path
from typing import Any

from ecomm.contracts.events import TOPIC_SUFFIXES
from ecomm.logging_utils import get_logger, log
from ecomm.spark.dedup import dedup_step
from ecomm.spark.rows import classify_value, dedup_key
from ecomm.spark.sink import (
    BatchRow,
    OffsetGapError,
    SinkConfig,
    load_offset_plan,
    process_batch,
    record_progress,
)
from ecomm.storage.local import LocalObjectStore

# Columns carried through the stateful step (must match OUT_FIELDS below).
COLUMNS = [
    "topic",
    "partition",
    "offset",
    "key",
    "value",
    "ts_ms",
    "status",
    "event_id",
    "event_type",
    "event_time_iso",
    "is_late",
    "error_code",
    "error_detail",
    "is_duplicate",
]


@dataclass(frozen=True)
class JobConfig:
    bootstrap: str
    topic_prefix: str
    lake_dir: Path
    checkpoint_dir: Path
    run_id: str
    source: str
    trigger: str
    max_offsets_per_trigger: int
    starting_offsets: str
    dedup_retention_hours: float
    late_threshold: timedelta
    shuffle_partitions: int
    driver_memory: str
    fail_on_data_loss: bool
    fail_after_commit_batch: int | None  # test hook: crash after committing this batch

    @property
    def topics(self) -> str:
        return ",".join(f"{self.topic_prefix}.{suffix}" for suffix in TOPIC_SUFFIXES)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ecomm.spark.job", description="Kafka to raw-zone streaming job."
    )
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    )
    parser.add_argument("--topic-prefix", default=os.environ.get("KAFKA_TOPIC_PREFIX", "commerce"))
    parser.add_argument("--lake-dir", type=Path, default=Path("/data/lake"))
    parser.add_argument("--checkpoint-dir", type=Path, default=Path("/data/checkpoints/ingest"))
    parser.add_argument(
        "--run-id",
        default="spark-main",
        help="prefix of every batch ID. If you delete the checkpoint, use a NEW run id: "
        "batch numbers restart at 0 and would clash with committed batches (refused).",
    )
    parser.add_argument("--source", default="commerce", help="source segment of object keys")
    parser.add_argument(
        "--trigger",
        default="available-now",
        help="'available-now' (process what is there, then stop) or 'processing-time:<n>s'",
    )
    parser.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument(
        "--dedup-retention-hours",
        type=float,
        default=24.0,
        help="how long an event_id is remembered, as event time on the Kafka timestamp. It must "
        "also be at least the time span of the data read at once: Spark drops rows older than "
        "its watermark, and the job then FAILS (messages stay in Kafka). Replaying a historical "
        "window therefore needs at least the window's length, e.g. 96 for 3 days.",
    )
    parser.add_argument(
        "--late-threshold-minutes",
        type=int,
        default=int(os.environ.get("LATE_EVENT_THRESHOLD_MINUTES", "60")),
    )
    parser.add_argument("--shuffle-partitions", type=int, default=2)
    parser.add_argument("--driver-memory", default="1g")
    parser.add_argument(
        "--no-fail-on-data-loss",
        action="store_true",
        help="continue if Kafka retention deleted unread data (default: fail loudly)",
    )
    parser.add_argument("--fail-after-commit-batch", type=int, help=argparse.SUPPRESS)
    return parser


def config_from_args(args: argparse.Namespace) -> JobConfig:
    if args.trigger != "available-now" and not args.trigger.startswith("processing-time:"):
        raise SystemExit("error: --trigger must be 'available-now' or 'processing-time:<n>s'")
    if args.max_offsets_per_trigger < 1:
        raise SystemExit("error: --max-offsets-per-trigger must be positive")
    return JobConfig(
        bootstrap=args.bootstrap,
        topic_prefix=args.topic_prefix,
        lake_dir=args.lake_dir,
        checkpoint_dir=args.checkpoint_dir,
        run_id=args.run_id,
        source=args.source,
        trigger=args.trigger,
        max_offsets_per_trigger=args.max_offsets_per_trigger,
        starting_offsets=args.starting_offsets,
        dedup_retention_hours=args.dedup_retention_hours,
        late_threshold=timedelta(minutes=args.late_threshold_minutes),
        shuffle_partitions=args.shuffle_partitions,
        driver_memory=args.driver_memory,
        fail_on_data_loss=not args.no_fail_on_data_loss,
        fail_after_commit_batch=args.fail_after_commit_batch,
    )


def make_sink_config(cfg: JobConfig) -> SinkConfig:
    stores = {
        "s3-local": LocalObjectStore(cfg.lake_dir / "s3-local", name="s3-local"),
        "gcs-local": LocalObjectStore(cfg.lake_dir / "gcs-local", name="gcs-local"),
    }
    control = LocalObjectStore(cfg.lake_dir / "control", name="control")
    return SinkConfig(source=cfg.source, run_id=cfg.run_id, stores=stores, control_store=control)


_OUR_ERROR_MESSAGE = re.compile(
    r"(?:[\w.]*\.)?(?:OffsetGapError|BatchConflictError|BatchCommitError): "
)


def summarize_failure(text: str, limit: int = 600) -> str:
    """One readable line for a failed query.

    Spark wraps a Python exception from ``foreachBatch`` in a long Java/Py4J stack trace; the
    useful part is our own error line, so prefer it over the first characters.
    """
    collapsed_all = " ".join(text.split())
    # "module.OffsetGapError: message" (not a traceback source line such as `raise OffsetGapError(`)
    match = _OUR_ERROR_MESSAGE.search(collapsed_all)
    if match:
        return collapsed_all[match.start() :][:limit]
    # A Python traceback ends with the exception that matters, so keep the tail, not the head.
    collapsed = " ".join(text.split())
    return collapsed if len(collapsed) <= limit else "..." + collapsed[-(limit - 3) :]


def _trigger_kwargs(trigger: str) -> dict[str, Any]:
    if trigger == "available-now":
        return {"availableNow": True}
    seconds = trigger.split(":", 1)[1].removesuffix("s")
    return {"processingTime": f"{int(seconds)} seconds"}


# --- the Spark part (imports pyspark) ---------------------------------------------------------------


def run(cfg: JobConfig) -> int:
    import pandas as pd
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

    logger = get_logger("ecomm.spark")
    sink_cfg = make_sink_config(cfg)
    retention_ms = int(cfg.dedup_retention_hours * 3_600_000)
    late_threshold = cfg.late_threshold

    spark = (
        SparkSession.builder.appName("ecomm-ingest")
        .master("local[2]")
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.prometheus.enabled", "true")  # /metrics/executors/prometheus on :4040
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    verdict_type = T.StructType(
        [
            T.StructField("status", T.StringType()),
            T.StructField("event_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
            T.StructField("event_time_iso", T.StringType()),
            T.StructField("is_late", T.BooleanType()),
            T.StructField("error_code", T.StringType()),
            T.StructField("error_detail", T.StringType()),
        ]
    )

    @F.udf(verdict_type)
    def classify_udf(value: Any, ts: Any) -> tuple[Any, ...]:
        arrival = ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts
        v = classify_value(bytes(value) if value is not None else None, arrival, late_threshold)
        return (
            v.status,
            v.event_id,
            v.event_type,
            v.event_time_iso,
            v.is_late,
            v.error_code,
            v.error_detail,
        )

    @F.udf(T.StringType())
    def key_udf(status: Any, event_id: Any, topic: Any, partition: Any, offset: Any) -> str:
        return dedup_key(status, event_id, topic, int(partition), int(offset))

    out_schema = T.StructType(
        [
            T.StructField("topic", T.StringType()),
            T.StructField("partition", T.IntegerType()),
            T.StructField("offset", T.LongType()),
            T.StructField("key", T.StringType()),
            T.StructField("value", T.BinaryType()),
            T.StructField("ts_ms", T.LongType()),
            T.StructField("status", T.StringType()),
            T.StructField("event_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
            T.StructField("event_time_iso", T.StringType()),
            T.StructField("is_late", T.BooleanType()),
            T.StructField("error_code", T.StringType()),
            T.StructField("error_detail", T.StringType()),
            T.StructField("is_duplicate", T.BooleanType()),
        ]
    )

    def dedup_fn(key: Any, pdf_iter: Any, state: GroupState) -> Any:
        if state.hasTimedOut:  # watermark passed last-seen + retention: forget this event_id
            state.remove()
            return
        rows: list[dict[str, Any]] = []
        for pdf in pdf_iter:
            rows.extend(pdf.to_dict("records"))
        last_seen = int(state.get[0]) if state.exists else None
        newest_ts_ms = max(int(r["ts_ms"]) for r in rows)  # event time = Kafka timestamp
        marked, new_state = dedup_step(rows, last_seen, newest_ts_ms)
        if new_state is None:
            state.remove()
        else:
            state.update((int(new_state),))
            # Spark rejects an expiry at or before the watermark (INVALID_TIMEOUT_TIMESTAMP).
            # That happens when the retention is shorter than the data's time spread; the
            # state then simply expires on the next batch instead of crashing the query.
            expiry = max(int(new_state) + retention_ms, state.getCurrentWatermarkMs() + 1)
            state.setTimeoutTimestamp(expiry)
        yield pd.DataFrame(marked, columns=COLUMNS)

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap)
        .option("subscribe", cfg.topics)
        .option("startingOffsets", cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        .option("failOnDataLoss", str(cfg.fail_on_data_loss).lower())
        .load()
    )
    classified = (
        reader.select(
            F.col("topic"),
            F.col("partition").cast("int").alias("partition"),
            F.col("offset").cast("long").alias("offset"),
            F.col("key").cast("string").alias("key"),
            F.col("value"),
            F.unix_millis(F.col("timestamp")).alias("ts_ms"),
            F.col("timestamp").alias("ts"),
            classify_udf(F.col("value"), F.col("timestamp")).alias("v"),
        )
        .select("topic", "partition", "offset", "key", "value", "ts_ms", "ts", "v.*")
        .withColumn("dedup_key", key_udf("status", "event_id", "topic", "partition", "offset"))
        # Event time = Kafka timestamp (arrival). The watermark lags the newest timestamp by
        # the retention, so only messages that arrive more than the retention behind the
        # newest one could be dropped by Spark; the sink's offset-gap check makes any such
        # drop loud. Event-time timeouts (unlike processing-time ones) let an available-now
        # query terminate.
        .withWatermark("ts", f"{int(retention_ms / 1000)} seconds")
    )
    deduped = classified.groupBy("dedup_key").applyInPandasWithState(
        dedup_fn,
        outputStructType=out_schema,
        stateStructType=T.StructType([T.StructField("last_seen_ms", T.LongType())]),
        outputMode="append",
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
    )

    def handle(batch_df: Any, batch_id: int) -> None:
        started = time.monotonic()
        rows = [
            BatchRow(
                topic=r["topic"],
                partition=int(r["partition"]),
                offset=int(r["offset"]),
                key=r["key"],
                value=bytes(r["value"]) if r["value"] is not None else b"",
                ts_ms=int(r["ts_ms"]),
                status=r["status"],
                event_id=r["event_id"],
                event_type=r["event_type"],
                event_time_iso=r["event_time_iso"],
                is_late=bool(r["is_late"]),
                error_code=r["error_code"],
                error_detail=r["error_detail"],
                is_duplicate=bool(r["is_duplicate"]),
            )
            for r in (row.asDict() for row in batch_df.collect())
        ]
        plan = None
        if sink_cfg.fail_on_offset_gap:
            try:
                plan = load_offset_plan(cfg.checkpoint_dir, batch_id)
            except (OSError, ValueError) as exc:  # no silent skip: completeness is unverifiable
                raise OffsetGapError(
                    f"cannot verify batch {batch_id} against Spark's planned offsets: {exc}"
                ) from exc
        outcome = process_batch(rows, batch_id, sink_cfg, plan)
        duration_ms = int((time.monotonic() - started) * 1000)
        if rows:
            record_progress(sink_cfg.control_store, cfg.source, outcome, duration_ms)
        log(
            logger,
            logging.INFO,
            "batch committed",
            duration_ms=duration_ms,
            **outcome.to_json_dict(),
        )
        if cfg.fail_after_commit_batch is not None and batch_id == cfg.fail_after_commit_batch:
            raise RuntimeError(f"injected failure after committing batch {batch_id} (test hook)")

    writer = (
        deduped.writeStream.queryName("ecomm-ingest")
        .foreachBatch(handle)
        .option("checkpointLocation", str(cfg.checkpoint_dir))
        .trigger(**_trigger_kwargs(cfg.trigger))
    )
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    log(
        logger,
        logging.INFO,
        "starting query",
        topics=cfg.topics,
        trigger=cfg.trigger,
        run_id=cfg.run_id,
        checkpoint=str(cfg.checkpoint_dir),
    )
    query = writer.start()
    try:
        while query.isActive:
            if stop.is_set():
                log(logger, logging.INFO, "stop requested; stopping query")
                query.stop()
                break
            query.awaitTermination(1)  # raises if the query failed
        exc = query.exception()
        if exc is not None:
            log(logger, logging.ERROR, "query failed", error=summarize_failure(str(exc)))
            return 1
    except Exception as exc:  # StreamingQueryException: a batch raised (e.g. OffsetGapError)
        log(logger, logging.ERROR, "query failed", error=summarize_failure(str(exc)))
        return 1
    finally:
        spark.stop()
    log(logger, logging.INFO, "query finished")
    return 0


def main(argv: list[str] | None = None) -> int:
    cfg = config_from_args(build_parser().parse_args(argv))
    return run(cfg)


if __name__ == "__main__":
    sys.exit(main())
