"""BI integration tests against the running stack (compose profile ``bi``).

Needs Metabase and its Postgres up, generated secrets in ``docker/bi/.env`` and a demo warehouse
(``ecomm-demo``). Skipped otherwise, so a laptop without Docker stays green.

What is proven, through Metabase's own API and a direct database login:
publishing is repeatable; provisioning is idempotent; every dashboard question runs without error;
the numbers on the dashboard equal the numbers in the DuckDB marts (the source of truth); the
connection Metabase uses can read the marts but cannot change or drop anything; the API refuses
anonymous callers.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import psycopg
import pytest

from ecomm.bi.dashboard_spec import (
    COLLECTION_NAME,
    DASHBOARD_NAME,
    DATABASE_NAME,
    QUESTIONS,
    QUESTIONS_BY_KEY,
)
from ecomm.bi.metabase import MetabaseClient, MetabaseError, provision
from ecomm.bi.publish import publish_marts
from ecomm.bi.settings import BiSettings, SettingsError

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
WAREHOUSE = REPO / "data" / "demo" / "warehouse.duckdb"


def _settings() -> BiSettings:
    try:
        return BiSettings.load(REPO / "docker" / "bi" / ".env")
    except SettingsError:
        pytest.skip("docker/bi/.env not generated (python docker/bi/generate_env.py)")


def _metabase_up(settings: BiSettings) -> bool:
    try:
        request = urllib.request.Request(settings.metabase_url + "/api/health")  # noqa: S310
        with urllib.request.urlopen(request, timeout=5) as response:  # noqa: S310 - loopback
            return bool(response.status == 200)
    except OSError:
        return False


@pytest.fixture(scope="module")
def settings() -> BiSettings:
    s = _settings()
    if not _metabase_up(s):
        pytest.skip("Metabase is not running (docker compose --profile bi up -d)")
    if not WAREHOUSE.is_file():
        pytest.skip("no demo warehouse (run ecomm-demo)")
    return s


@pytest.fixture(scope="module")
def provisioned(settings: BiSettings) -> dict[str, Any]:
    publish_marts(WAREHOUSE, settings.publisher_connection())
    return provision(settings, wait_seconds=60)


@pytest.fixture(scope="module")
def client(settings: BiSettings, provisioned: dict[str, Any]) -> MetabaseClient:
    c = MetabaseClient(settings.metabase_url)
    c.sign_in(settings)
    return c


@pytest.fixture(scope="module")
def marts() -> Iterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    yield con
    con.close()


def _cards_by_name(client: MetabaseClient, collection_id: int) -> dict[str, int]:
    payload = client.call("GET", f"/api/collection/{collection_id}/items?models=card")
    items = payload["data"] if isinstance(payload, dict) else payload
    return {item["name"]: item["id"] for item in items}


def _run(client: MetabaseClient, card_id: int) -> list[list[Any]]:
    result = client.call("POST", f"/api/card/{card_id}/query", {})
    assert result.get("status") == "completed", result.get("error")
    return result["data"]["rows"]  # type: ignore[no-any-return]


def test_provisioning_is_idempotent(
    settings: BiSettings, client: MetabaseClient, provisioned: dict[str, Any]
) -> None:
    again = provision(settings, wait_seconds=60)
    assert again["dashboard_id"] == provisioned["dashboard_id"]
    assert again["database_id"] == provisioned["database_id"]
    collections = [c for c in client.call("GET", "/api/collection") if c["name"] == COLLECTION_NAME]
    assert len(collections) == 1
    cards = _cards_by_name(client, again["collection_id"])
    assert len(cards) == len(QUESTIONS)
    databases = client.call("GET", "/api/database")
    databases = databases["data"] if isinstance(databases, dict) else databases
    assert [d["name"] for d in databases].count(DATABASE_NAME) == 1
    dashboard = client.call("GET", f"/api/dashboard/{again['dashboard_id']}")
    assert dashboard["name"] == DASHBOARD_NAME
    assert len(dashboard["dashcards"]) == len(QUESTIONS) + 1  # every question plus the notice


def test_the_dashboard_says_the_data_is_synthetic(
    client: MetabaseClient, provisioned: dict[str, Any]
) -> None:
    dashboard = client.call("GET", f"/api/dashboard/{provisioned['dashboard_id']}")
    texts = [
        c["visualization_settings"].get("text", "")
        for c in dashboard["dashcards"]
        if c.get("card_id") is None
    ]
    assert any("synthetic demo data" in t.lower() for t in texts)
    assert "synthetic" in DASHBOARD_NAME.lower()


def test_every_question_runs_and_data_questions_return_rows(
    client: MetabaseClient, provisioned: dict[str, Any]
) -> None:
    cards = _cards_by_name(client, provisioned["collection_id"])
    for question in QUESTIONS:
        rows = _run(client, cards[question.name])
        if question.key != "low_stock":  # a healthy warehouse may have nothing low on stock
            assert rows, f"{question.name} returned no rows"


def test_dashboard_numbers_equal_the_marts(
    client: MetabaseClient, provisioned: dict[str, Any], marts: duckdb.DuckDBPyConnection
) -> None:
    cards = _cards_by_name(client, provisioned["collection_id"])

    def run(key: str) -> list[list[Any]]:
        return _run(client, cards[QUESTIONS_BY_KEY[key].name])

    net_sales = marts.execute("select sum(net_sales) from analytics.mart_daily_sales").fetchone()[0]  # type: ignore[index]
    orders = marts.execute("select sum(order_count) from analytics.mart_daily_sales").fetchone()[0]  # type: ignore[index]
    assert float(run("kpi_net_sales")[0][0]) == pytest.approx(float(net_sales), abs=0.005)
    assert int(run("kpi_orders")[0][0]) == int(orders)
    assert float(run("kpi_aov")[0][0]) == pytest.approx(float(net_sales) / int(orders), abs=0.01)

    ok, terminal = marts.execute(
        "select sum(successful_payments), sum(terminal_payments) from analytics.mart_payment_health"
    ).fetchone()  # type: ignore[misc]
    assert float(run("kpi_payment_success")[0][0]) == pytest.approx(
        100 * int(ok) / int(terminal), abs=0.06
    )

    daily = marts.execute(
        "select order_date, net_sales from analytics.mart_daily_sales order by order_date"
    ).fetchall()
    shown = run("net_sales_by_day")
    assert len(shown) == len(daily)
    for (day, value), row in zip(daily, shown, strict=True):
        assert str(row[0]).startswith(day.isoformat())
        assert float(row[1]) == pytest.approx(float(value), abs=0.005)

    top = marts.execute(
        "select product_name, net_sales from analytics.mart_product_performance order by net_sales desc, product_name limit 10"
    ).fetchall()
    shown_top = run("top_products")
    assert [r[0] for r in shown_top] == [t[0] for t in top]

    low = marts.execute(
        "select count(*) from analytics.mart_inventory_risk where stock_status <> 'ok'"
    ).fetchone()[0]  # type: ignore[index]
    assert len(run("low_stock")) == min(int(low), 50)

    loaded = marts.execute(
        "select count(distinct batch_id) from raw.load_audit where status = 'SUCCEEDED'"
    ).fetchone()[0]  # type: ignore[index]
    assert int(run("data_freshness")[0][1]) == int(loaded)


def test_publishing_twice_replaces_rather_than_duplicates(
    settings: BiSettings, provisioned: dict[str, Any]
) -> None:
    first = publish_marts(WAREHOUSE, settings.publisher_connection())
    second = publish_marts(WAREHOUSE, settings.publisher_connection())
    assert first.rows == second.rows


def _reader() -> psycopg.Connection[Any]:
    s = _settings()
    return psycopg.connect(
        host=s.postgres_host, port=s.postgres_port, dbname="bi", user="bi_reader",
        password=s.reader_password, connect_timeout=10, autocommit=True,
    )  # fmt: skip


def test_the_reader_role_can_select_but_cannot_change_anything(
    settings: BiSettings, provisioned: dict[str, Any]
) -> None:
    with _reader() as conn:
        assert conn.execute("select count(*) from analytics.mart_daily_sales").fetchone()[0] > 0  # type: ignore[index]
        for statement in (
            "delete from analytics.mart_daily_sales",
            "insert into analytics.data_freshness (batches_loaded) values (1)",
            "drop table analytics.mart_daily_sales",
            "create table analytics.sneaky (x int)",
            "create table public.sneaky (x int)",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(statement)  # type: ignore[arg-type]


def test_metabase_connects_as_the_reader_and_never_exposes_the_password(
    settings: BiSettings, client: MetabaseClient, provisioned: dict[str, Any]
) -> None:
    database = client.call("GET", f"/api/database/{provisioned['database_id']}")
    assert database["details"]["user"] == "bi_reader"
    # Metabase masks stored secrets in API responses: no real password may come back.
    assert settings.reader_password not in str(database)
    assert settings.publisher_password not in str(database)


def test_the_api_refuses_anonymous_callers(
    settings: BiSettings, provisioned: dict[str, Any]
) -> None:
    anonymous = MetabaseClient(settings.metabase_url)
    for path in ("/api/card", "/api/dashboard", "/api/database"):
        with pytest.raises(MetabaseError, match="HTTP 401"):
            anonymous.call("GET", path, authenticated=False)
    # First-run setup is closed once an admin exists.
    props = anonymous.call("GET", "/api/session/properties", authenticated=False)
    assert props.get("has-user-setup") is True
