"""BigQuery warehouse client.

**Unverified against real BigQuery** (no GCP account available). DDL rendering
is unit-tested as text and the call flow is tested against a fake client. Run
a real smoke test before relying on it (see docs/operations.md).

Cost controls built in: bulk *load jobs* (free) instead of streaming inserts,
DATE partitioning + clustering on the event ledger, and auto-expiring scratch
tables. Credentials come from Application Default Credentials.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any

from ecomm.warehouse.client import WarehouseError
from ecomm.warehouse.schema import ColumnType, TableSpec, safe_identifier

_TYPES = {
    ColumnType.STRING: "STRING",
    ColumnType.INT: "INT64",
    ColumnType.BOOL: "BOOL",
    ColumnType.TIMESTAMP: "TIMESTAMP",
    ColumnType.DATE: "DATE",
}


def render_ddl(project: str, dataset: str, spec: TableSpec) -> str:
    columns = ",\n  ".join(
        f"{safe_identifier(c.name)} {_TYPES[c.type]}{'' if c.nullable else ' NOT NULL'}"
        for c in spec.columns
    )
    clauses = []
    if spec.partition_by:
        clauses.append(f"PARTITION BY {safe_identifier(spec.partition_by)}")
    if spec.cluster_by:
        clauses.append("CLUSTER BY " + ", ".join(safe_identifier(c) for c in spec.cluster_by))
    if spec.expires_hours:
        clauses.append(
            "OPTIONS(expiration_timestamp="
            f"TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {int(spec.expires_hours)} HOUR))"
        )
    tail = ("\n" + "\n".join(clauses)) if clauses else ""
    return (
        f"CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{safe_identifier(spec.name)}` (\n"
        f"  {columns}\n){tail}"
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):  # naive values are treated as UTC
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(value, date):
        return value.isoformat()
    return value


class BigQueryClient:
    dialect = "bigquery"

    def __init__(
        self, project: str, dataset: str, *, location: str | None = None, client: Any = None
    ) -> None:
        if not project or not dataset:
            raise WarehouseError("GCP_PROJECT_ID and BIGQUERY_DATASET are required")
        self.project = project
        self.dataset = dataset
        if client is None:
            try:
                from google.cloud import bigquery
            except ImportError as exc:
                raise WarehouseError(
                    "google-cloud-bigquery is not installed; run: uv sync --extra gcp"
                ) from exc
            client = bigquery.Client(project=project, location=location)
        self._client = client

    def ref(self, table: str) -> str:
        return f"`{self.project}.{self.dataset}.{safe_identifier(table)}`"

    def ensure_table(self, spec: TableSpec) -> None:
        self.execute(render_ddl(self.project, self.dataset, spec))

    def drop_table(self, table: str) -> None:
        self.execute(f"DROP TABLE IF EXISTS {self.ref(table)}")

    def load_rows(self, spec: TableSpec, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        try:
            from google.cloud import bigquery

            schema = [
                bigquery.SchemaField(
                    c.name, _TYPES[c.type], mode="NULLABLE" if c.nullable else "REQUIRED"
                )
                for c in spec.columns
            ]
            config = bigquery.LoadJobConfig(
                schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            payload = [{k: _json_value(v) for k, v in row.items()} for row in rows]
            job = self._client.load_table_from_json(
                payload, f"{self.project}.{self.dataset}.{spec.name}", job_config=config
            )
            job.result()
        except Exception as exc:
            raise WarehouseError(f"load into {spec.name} failed: {type(exc).__name__}") from exc

    def execute(self, sql: str) -> None:
        try:
            self._client.query(sql).result()
        except Exception as exc:
            raise WarehouseError(f"BigQuery statement failed: {type(exc).__name__}") from exc

    def query(self, sql: str) -> list[dict[str, Any]]:
        try:
            return [dict(row) for row in self._client.query(sql).result()]
        except Exception as exc:
            raise WarehouseError(f"BigQuery query failed: {type(exc).__name__}") from exc
