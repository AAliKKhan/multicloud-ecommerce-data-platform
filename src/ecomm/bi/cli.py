"""``ecomm-bi``: publish the marts to the BI database and provision the Metabase dashboard.

ecomm-bi publish [--demo-dir data/demo]    # copy the marts into Postgres (one transaction)
ecomm-bi provision                         # connection, questions, dashboard in Metabase
ecomm-bi all                               # publish, then provision
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ecomm.bi.metabase import MetabaseError, provision
from ecomm.bi.publish import PublishError, publish_marts
from ecomm.bi.settings import DEFAULT_ENV_FILE, BiSettings, SettingsError


def _publish(settings: BiSettings, demo_dir: Path) -> None:
    result = publish_marts(demo_dir / "warehouse.duckdb", settings.publisher_connection())
    for table, rows in result.rows.items():
        print(f"published analytics.{table}: {rows} rows")
    print(f"published at {result.published_at.isoformat()} UTC")


def _provision(settings: BiSettings, wait_seconds: float) -> None:
    summary = provision(settings, wait_seconds=wait_seconds)
    print(f"Metabase dashboard ready with {summary['cards']} questions: {summary['url']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish marts and provision Metabase")
    parser.add_argument("command", choices=["publish", "provision", "all"])
    parser.add_argument("--demo-dir", type=Path, default=Path("data/demo"))
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--wait-seconds", type=float, default=300.0)
    args = parser.parse_args(argv)

    try:
        settings = BiSettings.load(args.env_file)
        if args.command in ("publish", "all"):
            _publish(settings, args.demo_dir)
        if args.command in ("provision", "all"):
            _provision(settings, args.wait_seconds)
    except (SettingsError, PublishError, MetabaseError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
