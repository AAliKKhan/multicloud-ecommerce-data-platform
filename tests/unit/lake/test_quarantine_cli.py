from __future__ import annotations

import json
from pathlib import Path

import pytest

from ecomm.contracts import ViolationCode
from ecomm.lake.quarantine_cli import HINTS, format_report, load_quarantine, main, summarise
from ecomm.storage.local import LocalObjectStore


def records(demo_dir: Path) -> list[dict[str, object]]:
    return load_quarantine(LocalObjectStore(demo_dir / "lake" / "s3-local"))


def test_every_violation_code_has_an_operator_hint() -> None:
    assert {c.value for c in ViolationCode} <= set(HINTS)


def test_loads_all_quarantined_records(demo_dir: Path) -> None:
    summary = json.loads((demo_dir / "demo_summary.json").read_text())
    loaded = records(demo_dir)
    assert len(loaded) == summary["quarantined"] > 0
    assert all(
        {"error_code", "original_payload", "topic", "offset", "processed_at"} <= set(r)
        for r in loaded
    )


def test_summary_counts_add_up(demo_dir: Path) -> None:
    summary = summarise(records(demo_dir))  # type: ignore[arg-type]
    assert sum(summary["by_error_code"].values()) == summary["total"]
    assert sum(summary["by_topic"].values()) == summary["total"]
    assert summary["first_processed_at"] <= summary["last_processed_at"]


def test_report_truncates_payloads_and_limits_samples(demo_dir: Path) -> None:
    text = format_report(records(demo_dir), show=2, max_chars=30)  # type: ignore[arg-type]
    assert text.count("payload:") == 2
    assert "..." in text
    assert "By error code:" in text


def test_empty_quarantine_is_reported_cleanly() -> None:
    assert "Nothing to inspect" in format_report([], show=5)


def test_cli_filters_by_code_and_emits_json(
    demo_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = next(iter(summarise(records(demo_dir))["by_error_code"]))  # type: ignore[arg-type]
    assert main(["--demo-dir", str(demo_dir), "--code", code, "--json", "--show", "3"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert set(out["summary"]["by_error_code"]) == {code}
    assert len(out["samples"]) <= 3


def test_cli_text_output_and_missing_directory(
    demo_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["--demo-dir", str(demo_dir)]) == 0
    assert "Quarantined messages:" in capsys.readouterr().out
    assert main(["--demo-dir", str(tmp_path / "none")]) == 2
