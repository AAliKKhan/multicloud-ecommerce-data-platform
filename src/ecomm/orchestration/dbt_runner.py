"""Run ``dbt build`` against a pipeline data directory, one isolated run directory per run.

    uv run ecomm-dbt --demo-dir data/spark --run-id manual-1

Why a wrapper instead of calling ``dbt`` directly (this is what the Airflow DAGs run):

- **The seed must match the stream.** The product catalogue (``products.csv``) is produced
  together with the events. The run copies *that* file over the project's seed, and refuses
  to run without it (``--use-project-seed`` opts out explicitly), because a mismatching
  catalogue silently produces wrong product names.
- **Artifacts survive failures.** ``<demo-dir>/dbt-runs/<run_id>/`` holds a copy of the project,
  ``dbt.log``, ``target/`` (including ``run_results.json``) and ``summary.json``. A failed
  test never deletes them; only old *runs* beyond ``--keep`` are pruned.
- **No shared mutable state.** The repository's ``dbt/`` folder is only read, so two runs, or
  a developer's own ``dbt build``, never overwrite each other's ``target/``.
- **Idempotent.** ``dbt build`` rebuilds tables from the warehouse's raw ledger, so re-running
  with the same data gives the same result.

Exit codes: dbt's own (0 = every model and test passed or only warned; 1 = failure), 2 = bad
input (missing warehouse or seed). Warnings (e.g. orphan rows caused by quarantined events) do
not fail the run.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_KEEP = 10
_IGNORE = shutil.ignore_patterns("target", "logs", "dbt_packages", ".user.yml", "__pycache__")
_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")

CommandBuilder = Callable[[Path, Path, Sequence[str]], list[str]]


class DbtRunError(RuntimeError):
    """Invalid input for a dbt run (not a dbt failure)."""


def sanitize_run_id(run_id: str) -> str:
    """Make an arbitrary orchestrator run ID safe as a single directory name."""
    cleaned = _SAFE.sub("_", run_id).strip("._")
    if not cleaned:
        raise DbtRunError("run id is empty after sanitising")
    return cleaned[:120]


def default_command(project: Path, target: Path, extra: Sequence[str]) -> list[str]:
    return [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "build",
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(project),
        "--target",
        "duckdb",
        "--target-path",
        str(target),
        "--log-path",
        str(target.parent / "logs"),
        "--no-use-colors",
        *extra,
    ]


@dataclass(frozen=True)
class DbtRunResult:
    run_id: str
    run_dir: Path
    returncode: int
    counts: dict[str, int]

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "ok": self.ok,
            "returncode": self.returncode,
            "counts": self.counts,
            "run_dir": str(self.run_dir),
        }


def summarise_run_results(target: Path) -> dict[str, int]:
    """Counts by status from dbt's ``run_results.json`` (empty if dbt never got that far)."""
    path = target / "run_results.json"
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return dict(Counter(str(r.get("status")) for r in data.get("results", [])))


def prune_old_runs(runs_dir: Path, keep: int, current: Path) -> list[str]:
    """Delete the oldest run directories beyond ``keep`` (never the current one)."""
    if keep < 1:
        raise DbtRunError("--keep must be at least 1")
    dirs = sorted(
        (p for p in runs_dir.iterdir() if p.is_dir() and p != current),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    removed = []
    for old in dirs[keep - 1 :]:
        shutil.rmtree(old, ignore_errors=True)
        removed.append(old.name)
    return removed


def run_dbt_build(
    data_dir: Path,
    project_dir: Path,
    run_id: str,
    *,
    extra_args: Sequence[str] = (),
    use_project_seed: bool = False,
    keep: int = DEFAULT_KEEP,
    command_builder: CommandBuilder = default_command,
    echo: Callable[[str], None] = print,
) -> DbtRunResult:
    warehouse = data_dir / "warehouse.duckdb"
    if not warehouse.is_file():
        raise DbtRunError(f"{warehouse} not found; load the raw events first (ecomm-load)")
    seed = data_dir / "products.csv"
    if not seed.is_file() and not use_project_seed:
        raise DbtRunError(
            f"{seed} not found. The product seed must come from the same run as the events "
            "(ecomm-kafka produce --products-csv, or ecomm-demo). Use --use-project-seed to "
            "build with the repository's catalogue instead."
        )
    if not (project_dir / "dbt_project.yml").is_file():
        raise DbtRunError(f"{project_dir} is not a dbt project")

    safe_id = sanitize_run_id(run_id)
    runs_dir = data_dir / "dbt-runs"
    run_dir = runs_dir / safe_id
    if run_dir.exists():  # a retry of the same run: start from a clean copy, keep nothing stale
        shutil.rmtree(run_dir)
    project = run_dir / "project"
    shutil.copytree(project_dir, project, ignore=_IGNORE)
    if seed.is_file():
        shutil.copyfile(seed, project / "seeds" / "products.csv")
    target = run_dir / "target"
    target.mkdir(parents=True)

    env = {**os.environ, "DBT_DUCKDB_PATH": str(warehouse.resolve())}
    command = command_builder(project, target, extra_args)
    echo(json.dumps({"msg": "dbt run starting", "run_id": safe_id, "run_dir": str(run_dir)}))
    log_path = run_dir / "dbt.log"
    with (
        subprocess.Popen(  # noqa: S603 - argv built from fixed parts, no shell
            command,
            env=env,
            cwd=project,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        ) as proc,
        log_path.open("w", encoding="utf-8") as log,
    ):
        if proc.stdout is None:  # pragma: no cover - PIPE is always set above
            raise DbtRunError("could not capture dbt output")
        for line in proc.stdout:
            log.write(line)
            echo(line.rstrip("\n"))
        returncode = proc.wait()

    counts = summarise_run_results(target)
    result = DbtRunResult(safe_id, run_dir, returncode, counts)
    (run_dir / "summary.json").write_text(
        json.dumps(result.to_json_dict(), indent=2) + "\n", encoding="utf-8"
    )
    pruned = prune_old_runs(runs_dir, keep, run_dir)
    echo(json.dumps({**result.to_json_dict(), "msg": "dbt run finished", "pruned": pruned}))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ecomm-dbt", description=__doc__.split("\n")[0])
    parser.add_argument(
        "--demo-dir", type=Path, default=Path("data/demo"), help="pipeline data dir"
    )
    parser.add_argument("--project-dir", type=Path, default=Path("dbt"))
    parser.add_argument("--run-id", required=True, help="orchestrator run ID (sanitised for paths)")
    parser.add_argument("--use-project-seed", action="store_true")
    parser.add_argument("--keep", type=int, default=DEFAULT_KEEP, help="run directories to keep")
    args, extra = parser.parse_known_args(argv)

    try:
        result = run_dbt_build(
            args.demo_dir,
            args.project_dir,
            args.run_id,
            extra_args=[a for a in extra if a != "--"],
            use_project_seed=args.use_project_seed,
            keep=args.keep,
        )
    except DbtRunError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
