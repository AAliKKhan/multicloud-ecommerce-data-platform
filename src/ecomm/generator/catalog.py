"""Fictional product catalogue (reference data, not a Kafka topic)."""

from __future__ import annotations

import random
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

CENT = Decimal("0.01")

_CATEGORIES: dict[str, tuple[str, ...]] = {
    "home": ("Lamp", "Cushion", "Mug", "Blanket", "Clock"),
    "electronics": ("Charger", "Speaker", "Webcam", "Keyboard", "Headset"),
    "apparel": ("Jacket", "Scarf", "Sneakers", "Backpack", "Cap"),
    "outdoors": ("Tent", "Bottle", "Lantern", "Stove", "Compass"),
    "stationery": ("Notebook", "Pen Set", "Planner", "Stapler", "Marker Pack"),
}
_ADJECTIVES = ("Classic", "Compact", "Deluxe", "Eco", "Nimble", "Urban", "Vintage", "Prime")
_PRICE_RANGE = {
    "home": (8, 90),
    "electronics": (15, 220),
    "apparel": (12, 150),
    "outdoors": (10, 180),
    "stationery": (3, 40),
}


@dataclass(frozen=True)
class Product:
    product_id: str
    sku: str
    product_name: str
    category: str
    unit_price: Decimal
    currency: str
    is_active: bool

    def to_json_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["unit_price"] = str(self.unit_price)
        return data


def money(value: Decimal | float) -> Decimal:
    return Decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


def build_products(rng: random.Random, count: int) -> list[Product]:
    categories = sorted(_CATEGORIES)
    products = []
    for index in range(1, count + 1):
        category = categories[(index - 1) % len(categories)]
        noun = rng.choice(_CATEGORIES[category])
        low, high = _PRICE_RANGE[category]
        price = money(Decimal(rng.randrange(low * 100, high * 100 + 1)) / 100)
        products.append(
            Product(
                product_id=f"prod_{index:04d}",
                sku=f"SKU-{index:05d}",
                product_name=f"{rng.choice(_ADJECTIVES)} {noun}",
                category=category,
                unit_price=price,
                currency="USD",
                is_active=True,
            )
        )
    return products
