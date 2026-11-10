"""Turn generator emissions into consumer-side messages (no Kafka needed)."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from ecomm.generator import Emission
from ecomm.lake.ingest import SourceMessage


def messages_from_emissions(emissions: Iterable[Emission]) -> list[SourceMessage]:
    """Assign Kafka-like coordinates: one partition per topic, increasing offsets."""
    offsets: defaultdict[str, int] = defaultdict(int)
    messages = []
    for emission in emissions:
        offset = offsets[emission.topic]
        offsets[emission.topic] += 1
        messages.append(
            SourceMessage(
                topic=emission.topic,
                partition=0,
                offset=offset,
                key=emission.key,
                value=emission.value,
                arrival_time=emission.arrival_time,
            )
        )
    return messages
