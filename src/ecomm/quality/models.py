"""Quality check results and the report that groups them."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from ecomm.contracts.types import format_utc


class CheckStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"  # prerequisite not configured; never counts as a failure


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    status: CheckStatus
    observed: str
    expected: str
    detail: str = ""

    def to_json_dict(self) -> dict[str, str]:
        return {
            "check_id": self.check_id,
            "status": self.status.value,
            "observed": self.observed,
            "expected": self.expected,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class QualityReport:
    run_id: str
    checked_at: datetime
    results: list[CheckResult] = field(default_factory=list)

    def count(self, status: CheckStatus) -> int:
        return sum(1 for r in self.results if r.status is status)

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if r.status is CheckStatus.FAIL]

    @property
    def warned(self) -> list[CheckResult]:
        return [r for r in self.results if r.status is CheckStatus.WARN]

    @property
    def ok(self) -> bool:
        return not self.failed

    def get(self, check_id: str) -> CheckResult:
        for result in self.results:
            if result.check_id == check_id:
                return result
        raise KeyError(check_id)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "checked_at": format_utc(self.checked_at),
            "ok": self.ok,
            "counts": {s.value: self.count(s) for s in CheckStatus},
            "results": [r.to_json_dict() for r in self.results],
        }

    def format_text(self) -> str:
        lines = [f"Quality run {self.run_id} at {format_utc(self.checked_at)}"]
        for r in self.results:
            line = f"  [{r.status.value:<4}] {r.check_id}: observed {r.observed}"
            line += f" (expected {r.expected})"
            lines.append(line + (f" - {r.detail}" if r.detail else ""))
        counts = ", ".join(f"{s.value}={self.count(s)}" for s in CheckStatus)
        lines.append(f"Result: {'OK' if self.ok else 'FAILED'} ({counts})")
        return "\n".join(lines)
