"""``python -m ecomm.lake.ready_cli``: is there any committed batch to load?

Used by the Airflow DAG ``raw_to_warehouse_daily`` as its first task. It replaced a shell
``find`` on a local folder, which cannot see an S3-compatible bucket
(``ECOMM_S3_BACKEND=minio``), by asking the same lake factory every other tool uses.

Exit codes (a contract with the DAG, which skips on 99 and fails on anything else):

* ``0``  at least one batch is committed (its ``manifest.json`` exists: the commit marker);
* ``99`` nothing is committed yet, so there is nothing to load (the DAG skips the run);
* ``1``  the lake is missing, misconfigured or unreachable.

An unreachable lake is deliberately ``1`` and never ``99``: a skipped run looks healthy, so an
outage reported as "nothing to load" would go unnoticed.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ecomm.lake.manifest import manifest_prefix
from ecomm.lake.stores import lake_layout, open_lake
from ecomm.storage.interface import ObjectStore, StorageConfigError, StorageError

EXIT_READY = 0
EXIT_NOTHING = 99
EXIT_FAILED = 1


def check(store: ObjectStore, source: str) -> tuple[int, str]:
    """Classify the S3 side of the raw zone: ``(exit code, one-line message)``."""
    try:
        keys = store.list_objects(manifest_prefix(source))
    except StorageError as exc:
        return EXIT_FAILED, f"cannot list the committed batches of {store.name}: {exc}"
    manifests = [key for key in keys if key.endswith("/manifest.json")]
    if not manifests:
        return EXIT_NOTHING, f"no committed batches yet in {store.name}: nothing to load"
    return EXIT_READY, f"committed batches present in {store.name} (for example: {manifests[0]})"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ecomm.lake.ready_cli", description=__doc__.split("\n")[0]
    )
    parser.add_argument("--demo-dir", type=Path, default=Path("data/demo"))
    parser.add_argument("--source", default="commerce")
    args = parser.parse_args(argv)

    try:
        layout = lake_layout(args.demo_dir)
        if layout.s3_directory is not None and not layout.s3_directory.is_dir():
            # Checked BEFORE opening the stores: opening a local store creates its directory.
            print(
                f"raw lake not found at {layout.s3_directory}: run the Spark job first "
                "(docs/spark.md), or point ECOMM_DATA_DIR at the right directory",
                file=sys.stderr,
            )
            return EXIT_FAILED
        lake = open_lake(args.demo_dir)
    except StorageConfigError as exc:
        print(f"storage not configured: {exc}", file=sys.stderr)
        return EXIT_FAILED

    code, message = check(lake.s3, args.source)
    print(message, file=sys.stderr if code == EXIT_FAILED else sys.stdout)
    return code


if __name__ == "__main__":
    sys.exit(main())
