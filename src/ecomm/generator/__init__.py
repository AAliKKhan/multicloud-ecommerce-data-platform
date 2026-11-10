"""Seeded synthetic commerce data generator."""

from __future__ import annotations

from dataclasses import dataclass

from ecomm.generator.catalog import Product
from ecomm.generator.config import GeneratorConfig
from ecomm.generator.faults import Emission, FaultKind, inject_faults
from ecomm.generator.simulation import simulate


@dataclass(frozen=True)
class GenerationResult:
    emissions: list[Emission]  # what a producer sends, in arrival order
    products: list[Product]  # reference data (not a Kafka topic)


def generate(config: GeneratorConfig) -> GenerationResult:
    """Generate the full arrival stream for ``config``. Deterministic per seed."""
    simulation = simulate(config)
    return GenerationResult(
        emissions=inject_faults(simulation.events, config), products=simulation.products
    )


__all__ = ["Emission", "FaultKind", "GenerationResult", "GeneratorConfig", "Product", "generate"]
