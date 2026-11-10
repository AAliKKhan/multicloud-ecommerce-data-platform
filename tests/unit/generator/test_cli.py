import json
from pathlib import Path

import pytest

from ecomm.contracts import ContractViolation, parse_event
from ecomm.generator.cli import main

ARGS = ["--seed", "3", "--orders", "60", "--customers", "10", "--products", "8"]


def read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_writes_fixture_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([*ARGS, "--out", str(tmp_path)]) == 0
    summary = json.loads(capsys.readouterr().out)

    events = read_jsonl(tmp_path / "events.jsonl")
    labels = read_jsonl(tmp_path / "labels.jsonl")
    products = read_jsonl(tmp_path / "products.jsonl")
    assert len(events) == len(labels) == summary["total_emissions"]
    assert len(products) == 8
    assert json.loads((tmp_path / "summary.json").read_text()) == summary

    for event, label in zip(events, labels, strict=True):
        assert event["seq"] == label["seq"]
        value = str(event["value"])
        if label["fault"] == "malformed":
            with pytest.raises(ContractViolation):
                parse_event(value)
        else:
            parse_event(value)


def test_labels_are_not_part_of_the_message(tmp_path: Path) -> None:
    main([*ARGS, "--out", str(tmp_path)])
    for event in read_jsonl(tmp_path / "events.jsonl"):
        assert set(event) == {"seq", "arrival_time", "topic", "key", "value"}


def test_fixture_output_is_reproducible(tmp_path: Path) -> None:
    first, second = tmp_path / "a", tmp_path / "b"
    main([*ARGS, "--out", str(first)])
    main([*ARGS, "--out", str(second)])
    for name in ("events.jsonl", "labels.jsonl", "products.jsonl", "summary.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes()


def test_stdout_stream_mode(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([*ARGS, "--malformed-rate", "0", "--late-rate", "0"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines
    assert all("topic" in json.loads(line) for line in lines)


def test_invalid_arguments_return_exit_code_2(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--malformed-rate", "2"]) == 2
    assert "invalid arguments" in capsys.readouterr().err
