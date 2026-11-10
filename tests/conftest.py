"""Makes ``tests/`` importable (``import support``) and provides shared demo fixtures."""

from __future__ import annotations

import shutil
from datetime import timedelta
from pathlib import Path

import pytest

from ecomm.demo import run_batch_demo
from ecomm.generator import GeneratorConfig

_START = GeneratorConfig().start
DEMO_CONFIG = GeneratorConfig(
    seed=13,
    orders=150,
    customers=30,
    end=_START + timedelta(days=2),
    malformed_rate=0.04,
    late_rate=0.03,
    out_of_order_rate=0.05,
    duplicate_rate=0.04,
)


@pytest.fixture(scope="session")
def demo_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A pristine demo directory (built once per test session; never mutated)."""
    root = tmp_path_factory.mktemp("demo-template") / "demo"
    run_batch_demo(DEMO_CONFIG, root, batches=2)
    return root


@pytest.fixture()
def demo_dir(demo_template: Path, tmp_path: Path) -> Path:
    """A private, mutable copy of the demo directory for one test."""
    target = tmp_path / "demo"
    shutil.copytree(demo_template, target)
    return target
