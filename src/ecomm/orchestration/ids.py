"""Run identifiers.

Orchestrators pass IDs such as ``scheduled__2026-11-10T00:00:00+00:00`` that contain ``:`` and
``+``. Those are invalid in Windows file names and awkward in object keys, and an ID taken from a
user-supplied parameter must never be able to climb out of a directory. Everything that turns a
run ID into a path or an object key goes through :func:`sanitize_run_id`.
"""

from __future__ import annotations

import re

_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]+")
MAX_LENGTH = 120


class InvalidRunIdError(ValueError):
    """The run ID has no usable characters."""


def sanitize_run_id(run_id: str) -> str:
    cleaned = _UNSAFE.sub("_", run_id).strip("._")
    if not cleaned:
        raise InvalidRunIdError(f"run id {run_id!r} is empty after sanitising")
    return cleaned[:MAX_LENGTH]
