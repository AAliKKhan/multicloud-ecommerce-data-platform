from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from ecomm.lake.manifest import manifest_key
from ecomm.observability.cli import main as metrics_main
from ecomm.observability.cli import snapshot_for_demo_dir
from ecomm.observability.metrics import (
    Snapshot,
    build_registry,
    metric_names,
    snapshot_to_families,
)

REPO = Path(__file__).resolve().parents[3]
ALERTS = REPO / "monitoring" / "prometheus" / "alerts.yml"
DASHBOARD = REPO / "monitoring" / "grafana" / "dashboards" / "pipeline_health.json"
ALLOWED_LABELS = {"status", "event_type", "section"}


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


def fires(expr: str, snapshot: Snapshot) -> bool:
    match = EXPR.match(expr.strip())
    assert match, f"unsupported alert expression shape: {expr}"
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


def alert_rules() -> dict[str, str]:
    rules = yaml.safe_load(ALERTS.read_text(encoding="utf-8"))["groups"][0]["rules"]
    return {r["alert"]: r["expr"] for r in rules}


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
    referenced: set[str] = set()
    referenced |= set(re.findall(r"\becomm_[a-z_]+", ALERTS.read_text()))
    dashboard = json.loads(DASHBOARD.read_text())
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            referenced |= set(re.findall(r"\becomm_[a-z_]+", target["expr"]))
    assert referenced
    assert referenced <= exposed, referenced - exposed


def test_alert_rules_are_well_formed() -> None:
    rules = yaml.safe_load(ALERTS.read_text())["groups"][0]["rules"]
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
