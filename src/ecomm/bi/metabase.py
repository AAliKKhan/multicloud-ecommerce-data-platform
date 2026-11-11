"""Provision Metabase through its REST API: admin user, database connection, questions, dashboard.

Idempotent: every object is found by name and updated, or created. Running it twice leaves one
collection, one connection, one card per question and one dashboard. The connection uses the
read-only ``bi_reader`` role, so Metabase can never change the published marts.

Standard library HTTP only (the API is small and the calls are sequential).
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

from ecomm.bi.dashboard_spec import (
    COLLECTION_NAME,
    DASHBOARD_NAME,
    DATABASE_NAME,
    NOTICE,
    QUESTIONS,
    Question,
)
from ecomm.bi.settings import BiSettings

EXPECTED_TABLES = {
    "mart_daily_sales",
    "mart_payment_health",
    "mart_product_performance",
    "mart_inventory_risk",
    "data_freshness",
}


class MetabaseError(RuntimeError):
    """A Metabase API call failed or the instance is not in the expected state."""


class MetabaseClient:
    def __init__(self, base_url: str, *, timeout: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session: str | None = None

    def call(self, method: str, path: str, body: Any = None, *, authenticated: bool = True) -> Any:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base_url + path, data=data, method=method)  # noqa: S310
        request.add_header("Accept", "application/json")
        if data is not None:
            request.add_header("Content-Type", "application/json")
        if authenticated and self.session:
            request.add_header("X-Metabase-Session", self.session)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:  # noqa: S310
                raw = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:400]
            raise MetabaseError(f"{method} {path} -> HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise MetabaseError(f"{method} {path} failed: {exc}") from exc
        return json.loads(raw) if raw else None

    def wait_until_ready(self, seconds: float = 300.0) -> None:
        deadline = time.monotonic() + seconds
        last = "no answer"
        while time.monotonic() < deadline:
            try:
                if self.call("GET", "/api/health", authenticated=False).get("status") == "ok":
                    return
            except MetabaseError as exc:
                last = str(exc)
            time.sleep(3)
        raise MetabaseError(f"Metabase not ready after {seconds:.0f}s ({last})")

    def sign_in(self, settings: BiSettings) -> None:
        """Create the admin on first run (setup token), otherwise log in."""
        props = self.call("GET", "/api/session/properties", authenticated=False)
        token = props.get("setup-token")
        if token and not props.get("has-user-setup", False):
            result = self.call(
                "POST",
                "/api/setup",
                {
                    "token": token,
                    "user": {
                        "first_name": "Pipeline",
                        "last_name": "Admin",
                        "email": settings.admin_email,
                        "password": settings.admin_password,
                        "site_name": "E-commerce data platform (synthetic demo)",
                    },
                    "prefs": {
                        "site_name": "E-commerce data platform (synthetic demo)",
                        "site_locale": "en",
                        "allow_tracking": False,
                    },
                },
                authenticated=False,
            )
            self.session = result["id"]
            return
        result = self.call(
            "POST",
            "/api/session",
            {"username": settings.admin_email, "password": settings.admin_password},
            authenticated=False,
        )
        self.session = result["id"]


def _items(payload: Any) -> list[dict[str, Any]]:
    """Metabase list endpoints return a list or ``{"data": [...]}`` depending on version."""
    if isinstance(payload, dict):
        payload = payload.get("data", [])
    return [item for item in payload if isinstance(item, dict)]


def ensure_database(client: MetabaseClient, settings: BiSettings) -> int:
    details = {
        "host": settings.postgres_host_for_metabase,
        "port": settings.postgres_port_for_metabase,
        "dbname": "bi",
        "user": "bi_reader",
        "password": settings.reader_password,
        "ssl": False,
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": "analytics",
    }
    body = {
        "name": DATABASE_NAME,
        "engine": "postgres",
        "details": details,
        "is_full_sync": True,
        "is_on_demand": False,
        "auto_run_queries": True,
    }
    existing = [
        d for d in _items(client.call("GET", "/api/database")) if d.get("name") == DATABASE_NAME
    ]
    if existing:
        database_id = int(existing[0]["id"])
        client.call("PUT", f"/api/database/{database_id}", body)
    else:
        database_id = int(client.call("POST", "/api/database", body)["id"])
    return database_id


def wait_for_tables(client: MetabaseClient, database_id: int, seconds: float = 120.0) -> None:
    """Trigger a metadata sync and wait until every published table is visible to Metabase."""
    client.call("POST", f"/api/database/{database_id}/sync_schema")
    deadline = time.monotonic() + seconds
    seen: set[str] = set()
    while time.monotonic() < deadline:
        metadata = client.call("GET", f"/api/database/{database_id}/metadata")
        seen = {str(t.get("name")) for t in metadata.get("tables", [])}
        if seen >= EXPECTED_TABLES:
            return
        time.sleep(2)
    raise MetabaseError(
        f"Metabase did not see tables {sorted(EXPECTED_TABLES - seen)} after {seconds:.0f}s. "
        "Run `ecomm-bi publish` first."
    )


def ensure_collection(client: MetabaseClient) -> int:
    for collection in _items(client.call("GET", "/api/collection")):
        if collection.get("name") == COLLECTION_NAME and not collection.get("archived"):
            return int(collection["id"])
    created = client.call(
        "POST",
        "/api/collection",
        {"name": COLLECTION_NAME, "description": "Synthetic demo data. Curated marts only."},
    )
    return int(created["id"])


def _card_payload(question: Question, database_id: int, collection_id: int) -> dict[str, Any]:
    return {
        "name": question.name,
        "type": "question",
        "display": question.display,
        "collection_id": collection_id,
        "dataset_query": {
            "type": "native",
            "database": database_id,
            "native": {"query": question.sql},
        },
        "visualization_settings": question.settings,
    }


def ensure_cards(client: MetabaseClient, database_id: int, collection_id: int) -> dict[str, int]:
    items = _items(client.call("GET", f"/api/collection/{collection_id}/items?models=card"))
    by_name = {str(item["name"]): int(item["id"]) for item in items}
    ids: dict[str, int] = {}
    for question in QUESTIONS:
        payload = _card_payload(question, database_id, collection_id)
        if question.name in by_name:
            card_id = by_name[question.name]
            client.call("PUT", f"/api/card/{card_id}", payload)
        else:
            card_id = int(client.call("POST", "/api/card", payload)["id"])
        ids[question.key] = card_id
    return ids


def ensure_dashboard(client: MetabaseClient, collection_id: int, card_ids: dict[str, int]) -> int:
    items = _items(client.call("GET", f"/api/collection/{collection_id}/items?models=dashboard"))
    existing = [item for item in items if item.get("name") == DASHBOARD_NAME]
    if existing:
        dashboard_id = int(existing[0]["id"])
    else:
        dashboard_id = int(
            client.call(
                "POST",
                "/api/dashboard",
                {
                    "name": DASHBOARD_NAME,
                    "collection_id": collection_id,
                    "description": "Synthetic demo data: sales, payments, products, stock.",
                },
            )["id"]
        )
    dashcards: list[dict[str, Any]] = [
        {
            "id": -1,
            "card_id": None,
            "row": 0,
            "col": 0,
            "size_x": 24,
            "size_y": 2,
            "visualization_settings": {
                "virtual_card": {
                    "name": None,
                    "display": "text",
                    "visualization_settings": {},
                    "dataset_query": {},
                    "archived": False,
                },
                "text": NOTICE,
            },
            "parameter_mappings": [],
        }
    ]
    for index, question in enumerate(QUESTIONS, start=2):
        dashcards.append(
            {
                "id": -index,
                "card_id": card_ids[question.key],
                "row": question.row,
                "col": question.col,
                "size_x": question.width,
                "size_y": question.height,
                "visualization_settings": {},
                "parameter_mappings": [],
            }
        )
    client.call("PUT", f"/api/dashboard/{dashboard_id}", {"dashcards": dashcards})
    return dashboard_id


def provision(settings: BiSettings, *, wait_seconds: float = 300.0) -> dict[str, Any]:
    client = MetabaseClient(settings.metabase_url)
    client.wait_until_ready(wait_seconds)
    client.sign_in(settings)
    database_id = ensure_database(client, settings)
    wait_for_tables(client, database_id)
    collection_id = ensure_collection(client)
    card_ids = ensure_cards(client, database_id, collection_id)
    dashboard_id = ensure_dashboard(client, collection_id, card_ids)
    return {
        "database_id": database_id,
        "collection_id": collection_id,
        "dashboard_id": dashboard_id,
        "cards": len(card_ids),
        "url": f"{settings.metabase_url}/dashboard/{dashboard_id}",
    }
