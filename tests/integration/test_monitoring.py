"""Monitoring integration tests: Prometheus' own tooling, then the running stack.

Part 1 needs only Docker and the pinned ``prom/prometheus`` image: ``promtool`` validates the
scrape config and the alert rules, and runs ``alerts_test.yml`` through the real rule engine.

Part 2 needs the compose ``monitoring`` profile to be up (exporter, Kafka exporter, Prometheus,
Grafana) and a generated ``docker/monitoring/.env``. It checks, through the services' own HTTP APIs
on loopback, that every target is scraped, every rule loads, Grafana is provisioned and locked down,
and that every dashboard query runs without error. Each part is skipped when its prerequisite is
missing, so a laptop without Docker stays green.
"""

from __future__ import annotations

import base64
import json
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
PROM_IMAGE = "prom/prometheus:v3.5.0"
PROM_DIR = REPO / "monitoring" / "prometheus"
PROMETHEUS = "http://127.0.0.1:9090"
GRAFANA = "http://127.0.0.1:3000"
ENV_FILE = REPO / "docker" / "monitoring" / ".env"
DASHBOARD_UID = "ecomm-pipeline-health"

# Panels that legitimately have no series until the Spark job has recorded progress and Kafka holds
# data; everything else must return data once the pipeline has run.
MAY_BE_EMPTY = ("Spark", "Unprocessed", "not yet processed")


def docker(*args: str, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("docker") or "docker"
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [executable, *args], capture_output=True, text=True, timeout=timeout, check=False
    )


def promtool(*args: str) -> subprocess.CompletedProcess[str]:
    return docker(
        "run", "--rm", "--entrypoint", "promtool",
        "-v", f"{PROM_DIR}:/etc/prometheus:ro", PROM_IMAGE, *args,
    )  # fmt: skip


def have_prom_image() -> bool:
    return (
        shutil.which("docker") is not None
        and docker("image", "inspect", PROM_IMAGE).returncode == 0
    )


needs_promtool = pytest.mark.skipif(
    not have_prom_image(), reason=f"docker or the image {PROM_IMAGE} is not available"
)


# --- part 1: promtool -----------------------------------------------------------------------------


@needs_promtool
def test_promtool_accepts_the_scrape_config() -> None:
    result = promtool("check", "config", "/etc/prometheus/prometheus.yml")
    assert result.returncode == 0, result.stdout + result.stderr


@needs_promtool
def test_promtool_accepts_the_alert_rules() -> None:
    result = promtool("check", "rules", "/etc/prometheus/alerts.yml")
    assert result.returncode == 0, result.stdout + result.stderr


@needs_promtool
def test_the_real_rule_engine_passes_every_alert_scenario() -> None:
    result = promtool("test", "rules", "/etc/prometheus/alerts_test.yml")
    assert result.returncode == 0, result.stdout + result.stderr


def test_every_alert_has_a_scenario_where_it_fires_and_a_healthy_one_where_it_does_not() -> None:
    rules = yaml.safe_load((PROM_DIR / "alerts.yml").read_text(encoding="utf-8"))
    names = {r["alert"] for g in rules["groups"] for r in g["rules"]}
    tests = yaml.safe_load((PROM_DIR / "alerts_test.yml").read_text(encoding="utf-8"))["tests"]
    fires: set[str] = set()
    quiet: set[str] = set()
    for scenario in tests:
        for case in scenario["alert_rule_test"]:
            (fires if case["exp_alerts"] else quiet).add(case["alertname"])
    assert names - fires == set(), f"alerts never shown to fire: {names - fires}"
    assert names - quiet == set(), f"alerts never shown to stay quiet: {names - quiet}"


# --- part 2: the running stack ------------------------------------------------------------------------


def http_json(
    url: str, headers: dict[str, str] | None = None, body: dict[str, Any] | None = None
) -> Any:
    request = urllib.request.Request(url, method="POST" if body else "GET")  # noqa: S310 - loopback
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    data = None
    if body is not None:
        request.add_header("Content-Type", "application/json")
        data = json.dumps(body).encode()
    with urllib.request.urlopen(request, data=data, timeout=60) as response:  # noqa: S310
        return json.loads(response.read() or b"null")


def status_of(url: str, headers: dict[str, str] | None = None) -> int:
    request = urllib.request.Request(url)  # noqa: S310 - loopback
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return exc.code


def wait_for(condition: Callable[[], bool], seconds: float, what: str) -> None:
    deadline = time.monotonic() + seconds
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if condition():
                return
        except (urllib.error.URLError, OSError, KeyError, IndexError, ValueError) as exc:
            last = exc
        time.sleep(3)
    raise AssertionError(f"timed out waiting for {what}" + (f" ({last})" if last else ""))


def prom_query(expr: str) -> list[dict[str, Any]]:
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    return list(http_json(url)["data"]["result"])


@pytest.fixture(scope="module")
def stack() -> dict[str, str]:
    try:
        if status_of(f"{PROMETHEUS}/-/healthy") != 200 or status_of(f"{GRAFANA}/api/health") != 200:
            pytest.skip(
                "the monitoring stack is not running (docker compose --profile monitoring up)"
            )
    except OSError:
        pytest.skip("the monitoring stack is not running (docker compose --profile monitoring up)")
    if not ENV_FILE.is_file():
        pytest.skip("docker/monitoring/.env missing (python docker/monitoring/generate_env.py)")
    password = next(
        line.split("=", 1)[1]
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines()
        if line.startswith("GF_SECURITY_ADMIN_PASSWORD=")
    )
    token = base64.b64encode(f"admin:{password}".encode()).decode()
    wait_for(
        lambda: bool(prom_query('ecomm_exporter_ready{reason=""} == 1')),
        240,
        "the exporter to compute its first snapshot",
    )
    return {"Authorization": f"Basic {token}"}


def test_every_scrape_target_is_up(stack: dict[str, str]) -> None:
    def all_up() -> bool:
        targets = http_json(f"{PROMETHEUS}/api/v1/targets")["data"]["activeTargets"]
        return {t["labels"]["job"] for t in targets if t["health"] == "up"} == {
            "ecomm-pipeline", "kafka", "prometheus"
        }  # fmt: skip

    wait_for(all_up, 120, "ecomm-pipeline, kafka and prometheus targets to be up")


def test_every_rule_loads_healthy_and_none_fires_on_a_healthy_pipeline(
    stack: dict[str, str],
) -> None:
    def load_rules() -> list[dict[str, Any]]:
        groups = http_json(f"{PROMETHEUS}/api/v1/rules")["data"]["groups"]
        return [r for g in groups for r in g["rules"]]

    # A rule's health is "unknown" until its group has been evaluated once, which can lag the
    # exporter's first snapshot on a fresh start. Wait for that; a rule that stays unhealthy fails.
    wait_for(
        lambda: all(r.get("health") != "unknown" for r in load_rules()),
        120,
        "every alert rule to be evaluated once",
    )
    rules = load_rules()
    expected = yaml.safe_load((PROM_DIR / "alerts.yml").read_text(encoding="utf-8"))
    count = sum(len(g["rules"]) for g in expected["groups"])
    assert len(rules) == count, "Prometheus loaded a different rule set (restart it after editing)"
    unhealthy = {
        r["name"]: (r.get("health"), r.get("lastError")) for r in rules if r.get("health") != "ok"
    }
    assert unhealthy == {}, f"rules that failed to evaluate: {unhealthy}"
    # Give "for:" clauses a chance; PipelineStale etc. must still be quiet on a fresh demo run.
    firing = [r["name"] for r in rules if r.get("state") == "firing"]
    allowed = (
        {"ExporterCannotReadPipelineData"}
        if firing == ["ExporterCannotReadPipelineData"]
        else set()
    )
    assert set(firing) <= allowed, f"alerts firing on a healthy pipeline: {firing}"


def test_pipeline_and_kafka_numbers_are_real(stack: dict[str, str]) -> None:
    assert float(prom_query("sum(ecomm_batches)")[0]["value"][1]) >= 1
    assert float(prom_query("ecomm_parity_batches_mismatched")[0]["value"][1]) == 0
    assert float(prom_query("kafka_brokers")[0]["value"][1]) >= 1
    assert prom_query(
        "ecomm_exporter_snapshot_age_seconds"
    )  # the exporter reports its own freshness


def test_grafana_is_locked_down_and_provisioned(stack: dict[str, str]) -> None:
    assert status_of(f"{GRAFANA}/api/search") == 401  # no anonymous access
    default = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode()}
    assert status_of(f"{GRAFANA}/api/search", default) == 401  # the well-known login is rejected
    sources = http_json(f"{GRAFANA}/api/datasources", stack)
    assert [(s["uid"], s["type"]) for s in sources] == [("prometheus", "prometheus")]
    dash = http_json(f"{GRAFANA}/api/dashboards/uid/{DASHBOARD_UID}", stack)
    assert dash["meta"]["provisioned"] is True
    # `canSave` stays true for an admin; the protection is that the server refuses the write.
    edited = {**dash["dashboard"], "title": "TAMPERED"}
    save = urllib.request.Request(f"{GRAFANA}/api/dashboards/db", method="POST")  # noqa: S310
    save.add_header("Authorization", stack["Authorization"])
    save.add_header("Content-Type", "application/json")
    body = json.dumps({"dashboard": edited, "overwrite": True}).encode()
    with pytest.raises(urllib.error.HTTPError) as rejected:
        urllib.request.urlopen(save, data=body, timeout=30)  # noqa: S310 - loopback
    assert rejected.value.code == 400
    assert status_of(f"{GRAFANA}/api/dashboards/uid/{DASHBOARD_UID}", stack) == 200  # untouched


def test_every_dashboard_query_runs_and_core_panels_show_data(stack: dict[str, str]) -> None:
    dash = http_json(f"{GRAFANA}/api/dashboards/uid/{DASHBOARD_UID}", stack)["dashboard"]
    panels = [p for p in dash["panels"] if p["type"] != "row"]
    errors, empty = [], []
    for panel in panels:
        queries = [
            {
                "refId": t["refId"],
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "expr": t["expr"],
                "instant": True,
            }
            for t in panel["targets"]
        ]
        body = {"queries": queries, "from": "now-5m", "to": "now"}
        try:
            result = http_json(f"{GRAFANA}/api/ds/query", stack, body)
        except urllib.error.HTTPError as exc:
            errors.append((panel["title"], exc.code))
            continue
        frames = [f for r in result["results"].values() for f in r.get("frames", [])]
        if not any(f["data"]["values"] and f["data"]["values"][-1] for f in frames):
            empty.append(panel["title"])
    assert errors == []
    unexpected = [t for t in empty if not any(word in t for word in MAY_BE_EMPTY)]
    assert unexpected == [], f"panels with no data: {unexpected}"
