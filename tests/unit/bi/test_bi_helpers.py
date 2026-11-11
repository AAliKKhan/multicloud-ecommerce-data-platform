"""Unit tests of the BI helpers that need neither Docker nor Postgres."""

from __future__ import annotations

import importlib.util
import stat
import sys
from pathlib import Path

import pytest

from ecomm.bi.dashboard_spec import QUESTIONS, QUESTIONS_BY_KEY
from ecomm.bi.publish import MARTS, PublishError, postgres_type, publish_marts
from ecomm.bi.settings import BiSettings, SettingsError, read_env_file

REPO = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize(
    ("duck", "pg"),
    [
        ("DATE", "date"),
        ("BIGINT", "bigint"),
        ("HUGEINT", "bigint"),
        ("DOUBLE", "double precision"),
        ("FLOAT", "real"),
        ("VARCHAR", "text"),
        ("TIMESTAMP", "timestamp"),
        ("DECIMAL(38,2)", "numeric(38,2)"),
        ("decimal(18, 4)", "numeric(18,4)"),
    ],
)
def test_type_mapping(duck: str, pg: str) -> None:
    assert postgres_type(duck) == pg


def test_an_unknown_type_is_an_error_not_a_guess() -> None:
    with pytest.raises(PublishError, match="no Postgres mapping"):
        postgres_type("STRUCT(a INTEGER)")


def test_missing_warehouse_fails_before_touching_postgres(tmp_path: Path) -> None:
    with pytest.raises(PublishError, match="warehouse not found"):
        publish_marts(tmp_path / "nope.duckdb", {"host": "127.0.0.1", "port": 1})


def test_a_warehouse_without_marts_fails_before_touching_postgres(tmp_path: Path) -> None:
    import duckdb

    path = tmp_path / "empty.duckdb"
    duckdb.connect(str(path)).close()
    # Port 1 is closed: reaching Postgres would raise a different error than the mart message.
    with pytest.raises(PublishError, match="mart analytics.mart_daily_sales is not readable"):
        publish_marts(path, {"host": "127.0.0.1", "port": 1})


def test_only_curated_marts_are_published() -> None:
    assert set(MARTS) == {
        "mart_daily_sales",
        "mart_payment_health",
        "mart_product_performance",
        "mart_inventory_risk",
    }
    assert not any(name.startswith(("raw", "stg_", "int_")) for name in MARTS)


def test_env_file_parsing_ignores_comments_and_blank_lines(tmp_path: Path) -> None:
    path = tmp_path / ".env"
    path.write_text("# c\n\nA=1\nB = two\nnot a pair\n", encoding="utf-8")
    assert read_env_file(path) == {"A": "1", "B": "two"}
    assert read_env_file(tmp_path / "missing") == {}


def test_settings_say_how_to_create_missing_secrets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key in ("BI_PUBLISHER_PASSWORD", "BI_READER_PASSWORD", "METABASE_ADMIN_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(SettingsError, match="generate_env.py"):
        BiSettings.load(tmp_path / "missing.env")


def _generator() -> object:
    spec = importlib.util.spec_from_file_location(
        "bi_generate_env", REPO / "docker/bi/generate_env.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["bi_generate_env"] = module
    spec.loader.exec_module(module)
    return module


def test_generated_secrets_are_random_split_and_private(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generator = _generator()
    assert generator.main(["--dir", str(tmp_path)]) == 0  # type: ignore[attr-defined]
    app = read_env_file(tmp_path / ".env")
    postgres = read_env_file(tmp_path / ".env.postgres")
    assert set(postgres) == {"POSTGRES_PASSWORD"}  # the superuser password lives only here
    assert "POSTGRES_PASSWORD" not in app
    assert app["MB_DB_PASS"] == app["METABASE_DB_PASSWORD"]
    passwords = [v for k, v in app.items() if k.endswith("PASSWORD") or k == "MB_DB_PASS"]
    assert all(len(p) >= 20 for p in passwords)
    assert (
        len(
            {
                app["BI_PUBLISHER_PASSWORD"],
                app["BI_READER_PASSWORD"],
                app["METABASE_ADMIN_PASSWORD"],
            }
        )
        == 3
    )
    if sys.platform != "win32":
        assert stat.S_IMODE((tmp_path / ".env").stat().st_mode) == 0o600
    # An existing file is not overwritten; --force replaces it.
    before = (tmp_path / ".env").read_text(encoding="utf-8")
    generator.main(["--dir", str(tmp_path)])  # type: ignore[attr-defined]
    assert (tmp_path / ".env").read_text(encoding="utf-8") == before
    generator.main(["--dir", str(tmp_path), "--force"])  # type: ignore[attr-defined]
    assert (tmp_path / ".env").read_text(encoding="utf-8") != before


def test_dashboard_questions_are_unique_and_only_read_published_marts() -> None:
    assert len(QUESTIONS_BY_KEY) == len(QUESTIONS)
    assert len({q.name for q in QUESTIONS}) == len(QUESTIONS)  # names identify cards on re-runs
    allowed = {f"analytics.{name}" for name in MARTS} | {"analytics.data_freshness"}
    for question in QUESTIONS:
        sql = question.sql.lower()
        assert sql.lstrip().startswith("select"), question.key
        assert any(table in sql for table in allowed), question.key
        assert "raw." not in sql and "stg_" not in sql, question.key
        assert not any(w in sql for w in ("insert ", "update ", "delete ", "drop ")), question.key


def test_dashboard_layout_fits_the_grid_without_overlap() -> None:
    cells: set[tuple[int, int]] = set()
    for question in QUESTIONS:
        assert question.col >= 0 and question.col + question.width <= 24, question.key
        for r in range(question.row, question.row + question.height):
            for c in range(question.col, question.col + question.width):
                assert (r, c) not in cells, f"{question.key} overlaps at {(r, c)}"
                cells.add((r, c))
    assert min(q.row for q in QUESTIONS) >= 2  # rows 0-1 hold the synthetic-data notice


def test_spec_covers_every_item_of_the_suggested_dashboard() -> None:
    text = " ".join(q.name.lower() for q in QUESTIONS)
    for needle in (
        "net sales",
        "orders",
        "average order value",
        "payment",
        "top 10 products",
        "category",
        "stock",
        "freshness",
    ):
        assert needle in text, needle
