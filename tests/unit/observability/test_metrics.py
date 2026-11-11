from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from ecomm.lake.manifest import manifest_key
from ecomm.observability.cli import families_or_error, snapshot_for_demo_dir
from ecomm.observability.cli import main as metrics_main
from ecomm.observability.metrics import (
    Snapshot,
    build_registry,
    metric_names,
    snapshot_to_families,
)

REPO = Path(__file__).resolve().parents[3]
ALERTS = REPO / "monitoring" / "prometheus" / "alerts.yml"
DASHBOARD = REPO / "monitoring" / "grafana" / "dashboards" / "pipeline_health.json"
ALLOWED_LABELS = {"status", "event_type", "section", "topic", "partition", "reason"}
KAFKA_EXPORTER_METRICS = {"kafka_brokers", "kafka_topic_partition_current_offset"}


def samples(snapshot: Snapshot) -> list[tuple[str, dict[str, str], float]]:
    return [
        (s.name, dict(s.labels), s.value)
        for family in snapshot_to_families(snapshot)
        for s in family.samples
    ]


def value(snapshot: Snapshot, name: str, **labels: str) -> float:
    for sample_name, sample_labels, val in samples(snapshot):
        if sample_name == name and sample_labels == labels:
            return val
    raise KeyError((name, labels))


# --- a tiny evaluator for the alert expression shapes we use ------------------------------

EXPR = re.compile(
    r'^(?P<sum>sum\()?(?P<name>[a-z_]+)(?:\{(?P<label>\w+)(?P<op>=~|=)"(?P<pat>[^"]*)"\})?\)?'
    r"\s*>\s*(?P<threshold>[\d.]+)$"
)


def fires(expr: str, snapshot: Snapshot) -> bool | None:
    """Evaluate the simple expression shapes; ``None`` for the rest (promtool tests those)."""
    match = EXPR.match(expr.strip())
    if match is None:
        return None
    selected = []
    for name, labels, val in samples(snapshot):
        if name != match["name"]:
            continue
        if match["label"]:
            actual = labels.get(match["label"], "")
            ok = (
                re.fullmatch(match["pat"], actual) is not None
                if match["op"] == "=~"
                else actual == match["pat"]
            )
            if not ok:
                continue
        selected.append(val)
    threshold = float(match["threshold"])
    if match["sum"]:
        return sum(selected) > threshold
    return any(v > threshold for v in selected)


def all_rules() -> list[dict[str, Any]]:
    groups = yaml.safe_load(ALERTS.read_text(encoding="utf-8"))["groups"]
    return [rule for group in groups for rule in group["rules"]]


def alert_rules() -> dict[str, str]:
    return {r["alert"]: r["expr"] for r in all_rules()}


# --- snapshot content ---------------------------------------------------------------------


def test_snapshot_reflects_the_healthy_pipeline(demo_dir: Path) -> None:
    summary = json.loads((demo_dir / "demo_summary.json").read_text())
    snap = snapshot_for_demo_dir(demo_dir)

    assert snap.batches == {"COMPLETE": 2, "PARTIAL": 0, "FAILED": 0}
    assert snap.events_committed == summary["valid_events"]
    assert sum(snap.events_by_type.values()) == summary["valid_events"]
    assert snap.ingest_input == summary["messages"]
    assert snap.ingest_valid == summary["valid_events"]
    assert snap.ingest_duplicates == summary["duplicates_dropped"]
    assert snap.ingest_quarantined == summary["quarantined"]
    assert snap.ingest_late == summary["late_events_kept"]
    assert snap.load_attempts == {"SUCCEEDED": 2, "FAILED": 0}
    assert snap.load_rows_inserted == summary["valid_events"]
    assert snap.parity_checked == 2 and snap.parity_mismatched == 0
    assert snap.last_load_ts is not None
    assert snap.business_available is False  # dbt has not built the marts here
    assert snap.errors == dict.fromkeys(snap.errors, 0)


def test_quarantine_rate_is_quarantined_over_input(demo_dir: Path) -> None:
    snap = snapshot_for_demo_dir(demo_dir)
    assert value(snap, "ecomm_quarantine_rate") == pytest.approx(
        snap.ingest_quarantined / snap.ingest_input
    )
    assert value(snap, "ecomm_seconds_since_last_successful_load") < 3600


def test_text_exposition_is_valid_and_low_cardinality(demo_dir: Path) -> None:
    text = generate_latest(build_registry(snapshot_for_demo_dir(demo_dir))).decode()
    families = {f.name: f for f in text_string_to_metric_families(text)}
    assert "ecomm_batches" in families and "ecomm_quarantine_rate" in families
    for family in families.values():
        for sample in family.samples:
            assert set(sample.labels) <= ALLOWED_LABELS, sample
            # label values are small enumerations, never identifiers
            assert all(len(v) < 40 for v in sample.labels.values())


def test_business_metrics_are_namespaced_apart_from_pipeline_metrics(demo_dir: Path) -> None:
    names = metric_names(snapshot_for_demo_dir(demo_dir))
    business = {n for n in names if n.startswith("ecomm_business_")}
    assert business == {
        "ecomm_business_metrics_available",
        "ecomm_business_qualifying_orders",
        "ecomm_business_net_sales_usd",
    }


def test_cli_writes_a_textfile_and_reports_missing_data(
    demo_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "out" / "metrics.prom"
    assert metrics_main(["--demo-dir", str(demo_dir), "--textfile", str(out)]) == 0
    assert "ecomm_batches" in out.read_text()
    assert metrics_main(["--demo-dir", str(tmp_path / "none"), "--textfile", str(out)]) == 2
    assert "not found" in capsys.readouterr().out


# --- monitoring files stay consistent with the exporter -----------------------------------


def test_every_metric_in_alerts_and_dashboard_exists(demo_dir: Path) -> None:
    exposed = metric_names(snapshot_for_demo_dir(demo_dir))
    exposed |= {s.name for f in families_or_error(demo_dir) for s in f.samples}
    # Families that legitimately have no series yet (no Spark progress recorded) still exist:
    exposed |= {f.name for f in snapshot_to_families(snapshot_for_demo_dir(demo_dir))}
    referenced: set[str] = set()
    referenced |= set(re.findall(r"\becomm_[a-z_]+", ALERTS.read_text()))
    dashboard = json.loads(DASHBOARD.read_text())
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            referenced |= set(re.findall(r"\becomm_[a-z_]+", target["expr"]))
    assert referenced
    assert referenced <= exposed, referenced - exposed


def test_every_kafka_metric_in_alerts_and_dashboard_is_one_the_kafka_exporter_provides() -> None:
    text = ALERTS.read_text() + DASHBOARD.read_text()
    used = set(re.findall(r"\bkafka_[a-z_]+", text))
    assert used and used <= KAFKA_EXPORTER_METRICS, used - KAFKA_EXPORTER_METRICS


def test_alert_rules_are_well_formed() -> None:
    rules = all_rules()
    names = [r["alert"] for r in rules]
    assert len(names) == len(set(names))
    assert {
        "PipelineStale",
        "BatchesNotComplete",
        "HighQuarantineRate",
        "CloudParityMismatch",
    } <= set(names)
    for rule in rules:
        assert rule["expr"] and rule["for"]
        assert rule["labels"]["severity"] in {"warning", "critical"}
        assert rule["annotations"]["summary"]


def test_dashboard_is_well_formed() -> None:
    dashboard = json.loads(DASHBOARD.read_text())
    assert dashboard["uid"] and dashboard["title"]
    ids = [p["id"] for p in dashboard["panels"]]
    assert len(ids) == len(set(ids))
    for panel in dashboard["panels"]:
        grid = panel["gridPos"]
        assert grid["x"] >= 0 and grid["x"] + grid["w"] <= 24
        if panel["type"] != "row":
            assert panel["datasource"]["uid"] == "prometheus"
            assert panel["targets"]
    assert any(p["type"] == "row" and "Business" in p["title"] for p in dashboard["panels"])


def test_prometheus_and_grafana_provisioning_files_parse() -> None:
    prom = yaml.safe_load((REPO / "monitoring/prometheus/prometheus.yml").read_text())
    assert prom["rule_files"] and prom["scrape_configs"][0]["job_name"] == "ecomm-pipeline"
    ds = yaml.safe_load(
        (REPO / "monitoring/grafana/provisioning/datasources/prometheus.yml").read_text()
    )
    assert ds["datasources"][0]["uid"] == "prometheus"
    yaml.safe_load((REPO / "monitoring/grafana/provisioning/dashboards/dashboards.yml").read_text())


# --- deliberately failing scenario: do the alerts actually fire? --------------------------


def test_no_alert_fires_on_a_healthy_pipeline(demo_dir: Path) -> None:
    snap = snapshot_for_demo_dir(demo_dir)
    firing = [name for name, expr in alert_rules().items() if fires(expr, snap)]
    assert firing == []


def test_alerts_fire_in_a_simulated_failure(demo_dir: Path) -> None:
    lake = demo_dir / "lake"
    # 1. one batch PARTIAL in the ledger
    ledger = next((lake / "control" / "ledger").rglob("*.json"))
    data = json.loads(ledger.read_text())
    data["status"] = "PARTIAL"
    ledger.write_text(json.dumps(data))
    # 2. one cloud lost a manifest -> S3 and GCS disagree
    batch_id = next(
        p.parent.name.split("=")[1]
        for p in (lake / "gcs-local" / "manifests").rglob("manifest.json")
    )
    (lake / "gcs-local" / manifest_key("commerce", batch_id)).unlink()
    # 3. a failed warehouse load attempt
    import duckdb

    con = duckdb.connect(str(demo_dir / "warehouse.duckdb"))
    con.execute(
        "UPDATE raw.load_audit SET status = 'FAILED' WHERE attempt = 1 AND batch_id = ?", [batch_id]
    )
    con.close()
    # 4. a flood of quarantined messages
    stats = next((lake / "control" / "ingest").rglob("*.json"))
    s = json.loads(stats.read_text())
    s["quarantined"] = s["input_count"] // 2
    stats.write_text(json.dumps(s))
    # 5. a persisted quality run with failures
    quality_dir = lake / "control" / "quality" / "source=commerce"
    quality_dir.mkdir(parents=True)
    (quality_dir / "run_id=dq-x.json").write_text(
        json.dumps(
            {
                "counts": {"PASS": 5, "WARN": 1, "FAIL": 2, "SKIP": 0},
                "checked_at": "2026-11-10T00:00:00+00:00",
            }
        )
    )
    # 6. a corrupt ledger entry breaks one metrics section
    (lake / "control" / "ledger" / "source=commerce" / "broken.json").write_text("{not json")

    snap = snapshot_for_demo_dir(demo_dir)
    rules = alert_rules()
    for name in (
        "BatchesNotComplete",
        "WarehouseLoadFailures",
        "HighQuarantineRate",
        "CloudParityMismatch",
        "DataQualityChecksFailing",
        "MetricsCollectionBroken",
    ):
        assert fires(rules[name], snap), f"{name} should fire: {rules[name]}"
    assert not fires(rules["PipelineStale"], snap)  # loads are still recent

    stale = Snapshot(now=snap.now + timedelta(days=3), last_load_ts=snap.last_load_ts)
    assert fires(rules["PipelineStale"], stale)


def test_never_loaded_pipeline_is_stale() -> None:
    never = Snapshot(now=datetime(2026, 1, 1, tzinfo=UTC))
    assert fires(alert_rules()["PipelineStale"], never)


# --- exporter degradation and Spark progress ---------------------------------------------


def flat(families: list[Any]) -> list[tuple[str, dict[str, str], float]]:
    return [(s.name, dict(s.labels), s.value) for f in families for s in f.samples]


def test_a_missing_warehouse_gives_a_ready_zero_answer_instead_of_a_failed_scrape(
    tmp_path: Path,
) -> None:
    [(name, labels, value)] = flat(families_or_error(tmp_path))
    assert name == "ecomm_exporter_ready" and value == 0.0 and labels["reason"]


def test_a_corrupt_warehouse_file_is_reported_not_raised(tmp_path: Path) -> None:
    (tmp_path / "warehouse.duckdb").write_bytes(b"this is not a duckdb file" * 50)
    [(name, _, value)] = flat(families_or_error(tmp_path))
    assert (name, value) == ("ecomm_exporter_ready", 0.0)


def test_a_locked_warehouse_degrades_and_recovers_when_the_writer_lets_go(demo_dir: Path) -> None:
    import duckdb

    path = str(demo_dir / "warehouse.duckdb")
    writer = duckdb.connect(path)  # another connection with different settings: the lock case
    try:
        locked = {n: v for n, _, v in flat(families_or_error(demo_dir))}
        assert locked == {"ecomm_exporter_ready": 0.0}
    finally:
        writer.close()
    healthy = flat(families_or_error(demo_dir))
    assert ("ecomm_exporter_ready", {"reason": ""}, 1.0) in healthy
    assert any(n == "ecomm_batches" for n, _, _ in healthy)


def write_progress(demo_dir: Path, name: str, offsets: dict[str, list[int]]) -> None:
    folder = demo_dir / "lake" / "control" / "spark" / "source=commerce"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"batch_id={name}.json").write_text(
        json.dumps({"offsets": offsets, "recorded_at": "2026-11-10T12:00:00.000Z"})
    )


def test_spark_progress_becomes_next_offsets_per_topic_partition(demo_dir: Path) -> None:
    write_progress(demo_dir, "b0", {"commerce.orders/0": [0, 99], "commerce.orders/1": [0, 49]})
    write_progress(demo_dir, "b1", {"commerce.orders/0": [100, 149]})
    snap = snapshot_for_demo_dir(demo_dir)
    assert snap.spark_next_offsets == {("commerce.orders", 0): 150, ("commerce.orders", 1): 50}
    assert snap.spark_last_batch_ts == datetime(2026, 11, 10, 12, tzinfo=UTC).timestamp()
    assert snap.errors["spark"] == 0
    text = generate_latest(build_registry(snap)).decode()
    assert 'ecomm_spark_next_offset{partition="0",topic="commerce.orders"} 150.0' in text


def test_a_corrupt_spark_progress_file_only_breaks_its_own_section(demo_dir: Path) -> None:
    folder = demo_dir / "lake" / "control" / "spark" / "source=commerce"
    folder.mkdir(parents=True)
    (folder / "batch_id=bad.json").write_text("{not json")
    snap = snapshot_for_demo_dir(demo_dir)
    assert snap.errors["spark"] == 1 and snap.errors["lake"] == 0 and snap.events_committed > 0


def test_no_spark_progress_means_no_offset_series_not_zeros(demo_dir: Path) -> None:
    assert snapshot_for_demo_dir(demo_dir).spark_next_offsets == {}  # lag would be misleading


def test_dashboard_has_a_streaming_row_and_panels_never_overlap() -> None:
    panels = json.loads(DASHBOARD.read_text())["panels"]
    assert any(p["type"] == "row" and "Streaming" in p["title"] for p in panels)
    boxes = [(p["id"], p["gridPos"]) for p in panels]
    for i, (a, ga) in enumerate(boxes):
        for b, gb in boxes[i + 1 :]:
            overlap = (
                ga["x"] < gb["x"] + gb["w"]
                and gb["x"] < ga["x"] + ga["w"]
                and ga["y"] < gb["y"] + gb["h"]
                and gb["y"] < ga["y"] + ga["h"]
            )
            assert not overlap, f"panels {a} and {b} overlap"


def test_prometheus_targets_match_the_compose_services_and_exporter_port() -> None:
    services = yaml.safe_load((REPO / "docker-compose.yml").read_text())["services"]
    prom = yaml.safe_load((REPO / "monitoring/prometheus/prometheus.yml").read_text())
    targets = {j["job_name"]: j["static_configs"][0]["targets"][0] for j in prom["scrape_configs"]}
    assert targets["ecomm-pipeline"] == "ecomm-exporter:9108" and "ecomm-exporter" in services
    assert targets["kafka"] == "kafka-exporter:9308" and "kafka-exporter" in services
    dockerfile = (REPO / "docker" / "exporter" / "Dockerfile").read_text()
    assert '"--serve", "9108", "--addr", "0.0.0.0"' in dockerfile
