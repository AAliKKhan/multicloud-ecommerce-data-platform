"""Run the quality suite and persist the results (warehouse table + JSON report)."""

from __future__ import annotations

import json
from datetime import datetime

from ecomm.quality.checks import QualityContext, registered_checks, run_check
from ecomm.quality.models import QualityReport
from ecomm.storage.interface import ObjectStore
from ecomm.warehouse.client import WarehouseClient
from ecomm.warehouse.schema import QUALITY_RESULTS


def default_run_id(now: datetime) -> str:
    return f"dq-{now:%Y%m%dT%H%M%SZ}"


def run_quality_suite(ctx: QualityContext, run_id: str | None = None) -> QualityReport:
    """Evaluate every registered check. Never raises for a failing or broken check."""
    results = [run_check(check_id, fn, ctx) for check_id, fn in registered_checks()]
    return QualityReport(run_id or default_run_id(ctx.now), ctx.now, results)


def report_key(source: str, run_id: str) -> str:
    return f"quality/source={source}/run_id={run_id}.json"


def persist_report(
    report: QualityReport,
    client: WarehouseClient,
    control_store: ObjectStore | None = None,
    source: str = "commerce",
) -> None:
    """Store results in ``quality_results`` (queryable, joinable) and as JSON."""
    client.ensure_table(QUALITY_RESULTS)
    client.load_rows(
        QUALITY_RESULTS,
        [
            {
                "run_id": report.run_id,
                "check_id": r.check_id,
                "status": r.status.value,
                "observed": r.observed,
                "expected": r.expected,
                "detail": r.detail,
                "checked_at": report.checked_at,
            }
            for r in report.results
        ],
    )
    if control_store is not None:
        payload = json.dumps(report.to_json_dict(), indent=2) + "\n"
        control_store.write_object(report_key(source, report.run_id), payload.encode())
