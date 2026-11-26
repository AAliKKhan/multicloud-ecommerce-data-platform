"""Probe a BigQuery sandbox with this project's real client.

The free sandbox does not allow DML, so a MERGE failure is the expected result
and is not treated as a failed connection. This script does not enable billing,
create a key, call Terraform, or print credentials. It deletes the probe table
before it exits.
"""

from __future__ import annotations

import os
import sys

from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

from ecomm.warehouse.bigquery_client import BigQueryClient
from ecomm.warehouse.client import WarehouseError
from ecomm.warehouse.schema import Column, ColumnType, TableSpec

DATASET = "ecomm_sandbox_probe"
TABLE = TableSpec(
    name="sandbox_probe",
    columns=(
        Column("event_id", ColumnType.STRING, nullable=False),
        Column("payload", ColumnType.STRING, nullable=False),
    ),
    expires_hours=1,
)


def _safe(exc: BaseException) -> str:
    text = f"{type(exc).__name__}: {exc}"
    lowered = text.lower()
    if "bearer " in lowered or "private_key" in lowered or "refresh_token" in lowered:
        return type(exc).__name__
    return text


def main() -> int:
    project = os.environ.get("GCP_PROJECT_ID", "").strip()
    if not project:
        print("GCP_PROJECT_ID is not set. No cloud call was made.", file=sys.stderr)
        return 2
    location = os.environ.get("BIGQUERY_LOCATION", "US")
    print(f"project={project} location={location} dataset={DATASET}")
    try:
        service = bigquery.Client(project=project, location=location)
        warehouse = BigQueryClient(project, DATASET, location=location, client=service)
    except DefaultCredentialsError as exc:
        print(f"No Application Default Credentials. {_safe(exc)}", file=sys.stderr)
        return 2

    dataset = bigquery.Dataset(f"{project}.{DATASET}")
    dataset.location = location
    dataset.default_table_expiration_ms = 3_600_000
    created_dataset = False
    try:
        try:
            service.get_dataset(dataset.reference)
        except NotFound:
            service.create_dataset(dataset)
            created_dataset = True
            print("created dataset with a 1 hour default table expiration")

        print(f"query: {warehouse.query('SELECT 1 AS ok')}")
        warehouse.ensure_table(TABLE)
        warehouse.load_rows(
            TABLE, [{"event_id": "probe-1", "payload": "synthetic sandbox row"}]
        )
        target = warehouse.ref(TABLE.name)
        loaded = warehouse.query(
            # Only the qualified table identifier from warehouse.ref is interpolated.
            f"SELECT event_id FROM {target} WHERE event_id = 'probe-1'"  # noqa: S608
        )
        print(f"loaded: {loaded}")
        merge_sql = (
            "MERGE INTO "
            + target
            + " AS t USING "
            + target
            + " AS s ON t.event_id = s.event_id "
            + "WHEN NOT MATCHED THEN INSERT (event_id, payload) "
            + "VALUES (s.event_id, s.payload)"
        )
        try:
            warehouse.execute(merge_sql)
        except WarehouseError as exc:
            cause = exc.__cause__ or exc
            print(f"merge rejected, as the sandbox documents for DML: {_safe(cause)}")
        else:
            print(
                "merge succeeded. This project is not behaving like the documented sandbox. "
                "The probe table will still be deleted."
            )
        return 0
    except (WarehouseError, GoogleAPICallError, DefaultCredentialsError) as exc:
        print(f"probe failed: {_safe(exc)}", file=sys.stderr)
        return 1
    finally:
        try:
            service.delete_table(f"{project}.{DATASET}.{TABLE.name}", not_found_ok=True)
            print("deleted probe table")
            if created_dataset:
                service.delete_dataset(dataset.reference, delete_contents=True, not_found_ok=True)
                print("deleted probe dataset")
        except (GoogleAPICallError, DefaultCredentialsError) as exc:
            print(f"cleanup failed: {_safe(exc)}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
