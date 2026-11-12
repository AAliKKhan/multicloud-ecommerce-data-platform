"""``LocalObjectStore.list_objects`` must only look inside the part of the tree a prefix can match.

It used to walk and ``stat`` every file under the store root and filter by prefix afterwards, so
``list_objects("raw/")`` cost as much as listing the whole lake. On a Windows bind mount each ``stat``
is expensive: measured, a single listing took 10 to 56 seconds, and one metrics-exporter refresh,
which lists several times, took 130 to 237 seconds. Reading a single file took 0.03 s.

Two kinds of test: the results must equal a brute-force reference for every kind of prefix (the
change must not alter behaviour), and directories outside the prefix's subtree must never be visited
(deterministic proof of the speed-up, with no timing in it).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from ecomm.storage.local import LocalObjectStore

KEYS = [
    "raw/commerce/order_created/event_date=2026-11-01/event_hour=00/part-b0.jsonl",
    "raw/commerce/order_created/event_date=2026-11-02/event_hour=09/part-b1.jsonl",
    "raw/commerce/payment_captured/event_date=2026-11-01/event_hour=03/part-b0.jsonl",
    "raw/other/order_created/event_date=2026-11-01/event_hour=00/part-b0.jsonl",
    "raw/commerce/note.txt",
    "manifests/source=commerce/batch_id=b0/manifest.json",
    "manifests/source=commerce/batch_id=b1/manifest.json",
    "manifests/source=other/batch_id=x0/manifest.json",
    "quarantine/source=commerce/batch_id=b0/part-0.jsonl",
    "ledger/source=commerce/batch_id=b0.json",
    "rawx/lookalike-of-the-raw-prefix.txt",
    "top-level-file",
]


@pytest.fixture()
def store(tmp_path: Path) -> LocalObjectStore:
    local = LocalObjectStore(tmp_path / "lake")
    for key in KEYS:
        local.write_object(key, b"x", {"batch_id": "b"})  # writes a .meta.json sidecar as well
    (tmp_path / "lake" / "raw" / ".tmp-half-written").write_bytes(b"x")  # an interrupted write
    return local


def reference(keys: list[str], prefix: str) -> list[str]:
    return sorted(k for k in keys if k.startswith(prefix))


PREFIXES = [
    "",  # everything
    "raw/",  # a directory
    "raw/commerce/",  # a nested directory
    "raw/commerce/order_created/event_date=2026-11-01/",
    "raw",  # a partial component: also matches "rawx/..."
    "raw/comm",  # a partial last component
    "raw/commerce/order_created/event_date=2026-11-0",  # partial inside a directory name
    "manifests/source=commerce/",
    "manifests/source=commerce/batch_id=b",
    "raw/commerce/note.txt",  # an exact key
    "raw/commerce/note.txt/",  # a file used as a directory
    "top-level-file",
    "nothing/",  # does not exist
    "zzz",
    "raw/commerce/order_created/event_date=2026-11-01/event_hour=00/part-b0.jsonl",
]


@pytest.mark.parametrize("prefix", PREFIXES)
def test_listing_equals_the_brute_force_reference_for_every_kind_of_prefix(
    store: LocalObjectStore, prefix: str
) -> None:
    assert store.list_objects(prefix) == reference(KEYS, prefix)


def test_sidecars_and_interrupted_writes_are_never_listed(store: LocalObjectStore) -> None:
    listed = store.list_objects()
    assert listed == sorted(KEYS)
    assert not [k for k in listed if k.endswith(".meta.json") or ".tmp-" in k]


@pytest.mark.parametrize(
    "prefix",
    ["../", "raw/../", "/raw/", "raw//commerce/", "raw\\commerce", "./raw/", "raw/./commerce"],
)
def test_prefixes_that_no_valid_key_can_match_list_nothing_and_never_leave_the_root(
    store: LocalObjectStore, prefix: str, tmp_path: Path
) -> None:
    (tmp_path / "secret.txt").write_text("outside the store", encoding="utf-8")
    assert store.list_objects(prefix) == []


def test_only_the_subtree_of_the_prefix_is_visited(
    store: LocalObjectStore, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The point of the change, with no timing in it: other branches are never even opened."""
    visited: list[Path] = []
    real_scandir = os.scandir

    def spying_scandir(path: str | os.PathLike[str] = ".") -> object:
        visited.append(Path(path))
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", spying_scandir)
    root = tmp_path / "lake"

    assert len(store.list_objects("raw/commerce/order_created/")) == 2
    inside = {p.relative_to(root).as_posix() for p in visited}
    assert inside, "the spy saw nothing: the test would prove nothing"
    assert all(
        p == "raw/commerce/order_created" or p.startswith("raw/commerce/order_created/")
        for p in inside
    )
    assert not {
        "manifests",
        "quarantine",
        "ledger",
        "raw/other",
        "raw/commerce/payment_captured",
    } & {
        p.split("/")[0] if p.split("/")[0] != "raw" else "/".join(p.split("/")[:3]) for p in inside
    }

    visited.clear()
    store.list_objects("manifests/source=commerce/")
    assert {p.relative_to(root).as_posix().split("/")[0] for p in visited} == {"manifests"}
    assert not any("source=other" in p.as_posix() for p in visited)


def test_a_prefix_inside_a_missing_directory_opens_nothing(
    store: LocalObjectStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    visited: list[object] = []
    real_scandir = os.scandir
    monkeypatch.setattr(os, "scandir", lambda p=".": (visited.append(p), real_scandir(p))[1])
    assert store.list_objects("does/not/exist/") == []
    assert visited == []  # not even the root is opened


def test_an_empty_store_lists_nothing(tmp_path: Path) -> None:
    empty = LocalObjectStore(tmp_path / "empty")
    assert empty.list_objects() == [] and empty.list_objects("raw/") == []
