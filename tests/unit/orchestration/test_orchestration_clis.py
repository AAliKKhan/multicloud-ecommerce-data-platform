from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pytest

from ecomm.lake.manifest import manifest_key
from ecomm.lake.reconcile_cli import main as reconcile_main
from ecomm.orchestration.dbt_runner import (
    DbtRunError,
    prune_old_runs,
    run_dbt_build,
    sanitize_run_id,
    summarise_run_results,
)
from ecomm.warehouse.cli import main as load_main

# --- ecomm-reconcile -----------------------------------------------------------------------------


def test_reconcile_passes_on_a_healthy_lake_and_writes_a_report(
    demo_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert reconcile_main(["--demo-dir", str(demo_dir), "--run-id", "t1", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] and out["batches_checked"] == 2 and out["batches_mismatched"] == 0
    saved = demo_dir / "lake" / "control" / "reports" / "reconciliation" / "t1.json"
    assert json.loads(saved.read_text()) == out


def test_reconcile_exits_1_and_names_the_problem_when_a_cloud_lost_a_manifest(
    demo_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    gcs = demo_dir / "lake" / "gcs-local"
    manifest = next(gcs.rglob("manifest.json"))
    batch_id = manifest.parent.name.split("=", 1)[1]
    assert manifest == gcs / manifest_key("commerce", batch_id)
    manifest.unlink()
    assert reconcile_main(["--demo-dir", str(demo_dir)]) == 1
    text = capsys.readouterr().out
    assert "1 mismatched" in text and batch_id in text and "manifest missing" in text


def test_reconcile_detects_a_tampered_object(demo_dir: Path) -> None:
    victim = next((demo_dir / "lake" / "s3-local" / "raw").rglob("*.jsonl"))
    victim.write_bytes(victim.read_bytes().replace(b'"ingest"', b'"INGEST"', 1))
    assert reconcile_main(["--demo-dir", str(demo_dir)]) == 1


def test_reconcile_reports_a_missing_lake_clearly(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert reconcile_main(["--demo-dir", str(tmp_path)]) == 2
    assert "not found" in capsys.readouterr().out


# --- ecomm-load --only-new --------------------------------------------------------------------------


def test_load_only_new_skips_batches_that_already_succeeded(
    demo_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert load_main(["--demo-dir", str(demo_dir), "--only-new", "--run-id", "daily-1"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] and report["batches_loaded"] == 0
    assert len(report["batches_skipped"]) == 2 and report["rows_inserted"] == 0


def test_load_without_only_new_rechecks_everything_and_inserts_nothing(
    demo_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert load_main(["--demo-dir", str(demo_dir), "--run-id", "full-1"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["batches_loaded"] == 2 and report["rows_inserted"] == 0
    assert report["rows_duplicate"] == report["rows_read"] > 0


def test_load_rejects_a_reversed_date_range(
    demo_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        load_main(["--demo-dir", str(demo_dir), "--start", "2026-11-05", "--end", "2026-11-01"])
        == 2
    )
    assert "before --start" in capsys.readouterr().out


@pytest.mark.parametrize("bad", ["2026-13-01", "yesterday", "2026-11-01; rm -rf /"])
def test_load_rejects_malformed_dates_before_touching_anything(demo_dir: Path, bad: str) -> None:
    with pytest.raises(SystemExit) as exc:
        load_main(["--demo-dir", str(demo_dir), "--start", bad])
    assert exc.value.code == 2  # argparse usage error


# --- the dbt runner (with a fake dbt) ------------------------------------------------------------------


def fake_dbt(exit_code: int = 0, statuses: tuple[str, ...] = ("pass", "pass", "warn")):  # type: ignore[no-untyped-def]
    """A stand-in for dbt: writes run_results.json into --target-path and exits."""

    def builder(project: Path, target: Path, extra: object) -> list[str]:
        script = (
            "import json, pathlib, sys;"
            f"t = pathlib.Path(r'{target}'); t.mkdir(exist_ok=True);"
            f"(t / 'run_results.json').write_text(json.dumps({{'results': "
            f"[{{'status': s}} for s in {list(statuses)!r}]}}));"
            "print('seed products.csv:', (pathlib.Path(r'"
            + str(project)
            + "') / 'seeds' / 'products.csv').read_text().strip());"
            "print('DBT_DUCKDB_PATH', __import__('os').environ['DBT_DUCKDB_PATH']);"
            f"sys.exit({exit_code})"
        )
        return [sys.executable, "-c", script]

    return builder


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    p = tmp_path / "dbtproj"
    (p / "seeds").mkdir(parents=True)
    (p / "dbt_project.yml").write_text("name: x\n")
    (p / "seeds" / "products.csv").write_text("REPO-CATALOGUE\n")
    (p / "target").mkdir()
    (p / "target" / "stale.txt").write_text("must not be copied")
    return p


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    (d / "warehouse.duckdb").write_bytes(b"")
    (d / "products.csv").write_text("STREAM-CATALOGUE\n")
    return d


def test_run_ids_from_airflow_become_safe_directory_names() -> None:
    assert (
        sanitize_run_id("scheduled__2026-11-10T00:00:00+00:00")
        == "scheduled__2026-11-10T00_00_00_00_00"
    )
    assert "/" not in sanitize_run_id("../../etc/passwd") and ".." not in sanitize_run_id("../../x")
    with pytest.raises(DbtRunError):
        sanitize_run_id("///")


def test_a_run_uses_the_streams_seed_and_the_target_warehouse(
    data_dir: Path, project: Path
) -> None:
    lines: list[str] = []
    result = run_dbt_build(data_dir, project, "r1", command_builder=fake_dbt(), echo=lines.append)
    assert result.ok and result.counts == {"pass": 2, "warn": 1}
    out = "\n".join(lines)
    assert "STREAM-CATALOGUE" in out and "REPO-CATALOGUE" not in out
    assert str((data_dir / "warehouse.duckdb").resolve()) in out
    assert (project / "seeds" / "products.csv").read_text() == "REPO-CATALOGUE\n"  # repo untouched
    assert not (result.run_dir / "project" / "target" / "stale.txt").exists()
    assert json.loads((result.run_dir / "summary.json").read_text())["ok"] is True


def test_a_failed_run_keeps_its_logs_and_artifacts_and_reports_the_exit_code(
    data_dir: Path, project: Path
) -> None:
    result = run_dbt_build(
        data_dir,
        project,
        "r2",
        command_builder=fake_dbt(1, ("pass", "fail", "error")),
        echo=lambda _: None,
    )
    assert not result.ok and result.returncode == 1
    assert result.counts == {"pass": 1, "fail": 1, "error": 1}
    assert (result.run_dir / "dbt.log").read_text().strip()
    assert (result.run_dir / "target" / "run_results.json").is_file()
    assert json.loads((result.run_dir / "summary.json").read_text())["ok"] is False


def test_retrying_the_same_run_id_starts_clean(data_dir: Path, project: Path) -> None:
    first = run_dbt_build(data_dir, project, "r3", command_builder=fake_dbt(1), echo=lambda _: None)
    (first.run_dir / "leftover.txt").write_text("from the failed attempt")
    second = run_dbt_build(
        data_dir, project, "r3", command_builder=fake_dbt(0), echo=lambda _: None
    )
    assert second.ok and not (second.run_dir / "leftover.txt").exists()


def test_missing_inputs_are_clear_errors_not_dbt_failures(tmp_path: Path, project: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(DbtRunError, match="load the raw events first"):
        run_dbt_build(empty, project, "x", command_builder=fake_dbt())
    (empty / "warehouse.duckdb").write_bytes(b"")
    with pytest.raises(DbtRunError, match="--use-project-seed"):
        run_dbt_build(empty, project, "x", command_builder=fake_dbt())
    with pytest.raises(DbtRunError, match="not a dbt project"):
        run_dbt_build(empty, empty, "x", use_project_seed=True, command_builder=fake_dbt())


def test_project_seed_can_be_chosen_explicitly(tmp_path: Path, project: Path) -> None:
    data = tmp_path / "d"
    data.mkdir()
    (data / "warehouse.duckdb").write_bytes(b"")
    lines: list[str] = []
    result = run_dbt_build(
        data, project, "p", use_project_seed=True, command_builder=fake_dbt(), echo=lines.append
    )
    assert result.ok and "REPO-CATALOGUE" in "\n".join(lines)


def test_only_the_newest_runs_are_kept(data_dir: Path, project: Path) -> None:
    for i in range(5):
        run_dbt_build(
            data_dir, project, f"run{i}", keep=3, command_builder=fake_dbt(), echo=lambda _: None
        )
        time.sleep(0.05)
        os.utime(data_dir / "dbt-runs" / f"run{i}", (time.time() + i, time.time() + i))
    kept = sorted(p.name for p in (data_dir / "dbt-runs").iterdir())
    assert kept == ["run2", "run3", "run4"]
    with pytest.raises(DbtRunError):
        prune_old_runs(data_dir / "dbt-runs", 0, data_dir / "dbt-runs" / "run4")


def test_summary_without_results_is_empty(tmp_path: Path) -> None:
    assert summarise_run_results(tmp_path) == {}
