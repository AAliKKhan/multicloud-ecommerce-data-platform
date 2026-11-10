"""``ecomm-generate`` command line interface.

Examples::

    # Fixture directory (events, labels, products, summary)
    uv run ecomm-generate --out data/fixtures/demo --orders 200 --seed 42

    # Paced stream of records on stdout (10 events per second)
    uv run ecomm-generate --orders 50 --rate 10
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import ValidationError

from ecomm.generator import GeneratorConfig, generate
from ecomm.generator.config import DEFAULT_START
from ecomm.generator.output import emission_record, write_fixture


def add_generator_arguments(parser: argparse.ArgumentParser) -> None:
    """Generator options shared by ``ecomm-generate`` and ``ecomm-kafka produce``."""
    parser.add_argument("--seed", type=int, default=42, help="random seed (default 42)")
    parser.add_argument("--orders", type=int, default=200, help="number of orders")
    parser.add_argument("--customers", type=int, default=50)
    parser.add_argument("--products", type=int, default=30)
    parser.add_argument("--locations", type=int, default=3)
    parser.add_argument("--start", default=DEFAULT_START.isoformat(), help="ISO-8601 start (UTC)")
    parser.add_argument("--days", type=float, default=1.0, help="length of the time window")
    parser.add_argument("--malformed-rate", type=float, default=0.02)
    parser.add_argument("--late-rate", type=float, default=0.02)
    parser.add_argument("--out-of-order-rate", type=float, default=0.05)
    parser.add_argument("--duplicate-rate", type=float, default=0.03)
    parser.add_argument("--late-threshold-minutes", type=int, default=60)


def config_from_args(args: argparse.Namespace) -> GeneratorConfig:
    """Build a validated config; raises ``ValidationError``/``ValueError`` on bad input."""
    start = datetime.fromisoformat(args.start)
    return GeneratorConfig(
        seed=args.seed,
        start=start,
        end=start + timedelta(days=args.days),
        orders=args.orders,
        customers=args.customers,
        products=args.products,
        locations=args.locations,
        malformed_rate=args.malformed_rate,
        late_rate=args.late_rate,
        out_of_order_rate=args.out_of_order_rate,
        duplicate_rate=args.duplicate_rate,
        late_threshold_minutes=args.late_threshold_minutes,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ecomm-generate", description="Generate synthetic e-commerce events."
    )
    add_generator_arguments(parser)
    parser.add_argument("--out", type=Path, help="write fixture files to this directory")
    parser.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="stdout mode only: events per second (0 = as fast as possible)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = config_from_args(args)
    except (ValidationError, ValueError) as exc:
        print(f"error: invalid arguments: {exc}", file=sys.stderr)
        return 2

    result = generate(config)
    if args.out is not None:
        summary = write_fixture(result, args.out)
        print(json.dumps(summary, indent=2))
        print(f"wrote fixtures to {args.out}", file=sys.stderr)
        return 0

    delay = 1.0 / args.rate if args.rate > 0 else 0.0
    for emission in result.emissions:
        print(json.dumps(emission_record(emission), separators=(",", ":")), flush=True)
        if delay:
            time.sleep(delay)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
