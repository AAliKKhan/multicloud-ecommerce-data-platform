"""Local filesystem adapter (also the stand-in for S3/GCS in local mode).

Metadata is kept in a ``<key>.meta.json`` sidecar next to the object. Sidecars
are hidden from ``list_objects``. Writes are atomic (temp file + rename) so a
crash never leaves a half-written object visible.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

from ecomm.storage.interface import ObjectNotFoundError, StorageError, validate_key

_META_SUFFIX = ".meta.json"
_OBJECT_MODE = 0o644


class LocalObjectStore:
    def __init__(self, root: Path, name: str = "local") -> None:
        self.name = name
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        path = (self.root / validate_key(key)).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError(f"key escapes the store root: {key!r}")
        return path

    def write_object(
        self, key: str, data: bytes, metadata: Mapping[str, str] | None = None
    ) -> None:
        path = self._path(key)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._atomic_write(path, data)
            sidecar = path.with_name(path.name + _META_SUFFIX)
            if metadata:
                self._atomic_write(sidecar, json.dumps(dict(metadata)).encode())
            else:
                sidecar.unlink(missing_ok=True)
        except OSError as exc:
            raise StorageError(f"write failed for {key}: {exc}") from exc

    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            # mkstemp creates 0600 files. This store holds synthetic demo data and is also
            # written by containers running as root (the Spark job) and read by the host
            # user, so files must be world-readable like any normally created file.
            os.chmod(tmp, _OBJECT_MODE)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def read_object(self, key: str) -> bytes:
        path = self._path(key)
        try:
            return path.read_bytes()
        except FileNotFoundError as exc:
            raise ObjectNotFoundError(key) from exc
        except OSError as exc:
            raise StorageError(f"read failed for {key}: {exc}") from exc

    def read_metadata(self, key: str) -> dict[str, str]:
        path = self._path(key)
        if not path.is_file():
            raise ObjectNotFoundError(key)
        sidecar = path.with_name(path.name + _META_SUFFIX)
        if not sidecar.is_file():
            return {}
        loaded: dict[str, str] = json.loads(sidecar.read_text(encoding="utf-8"))
        return loaded

    def list_objects(self, prefix: str = "") -> list[str]:
        """Keys under ``prefix``, sorted. Only the part of the tree the prefix can match is read.

        The directory part of the prefix (everything up to its last ``/``) is where the walk starts,
        so ``raw/`` never opens ``manifests/``. This used to walk and ``stat`` every file under the
        root and filter afterwards; on a Windows bind mount that took 10 to 56 seconds per call.
        ``os.walk`` classifies entries from the directory listing itself, with no per-file ``stat``.
        """
        directory, _, _ = prefix.rpartition("/")
        parts = directory.split("/") if directory else []
        # No valid key has an empty, "." or ".." segment or a backslash, so such a prefix matches
        # nothing (and must never be used to walk outside the root).
        if (
            "\\" in prefix
            or any(part in ("", ".", "..") for part in parts)
            or prefix.startswith("/")
        ):
            return []
        start = self.root.joinpath(*parts)
        if not start.is_dir():
            return []
        keys = []
        for current, _dirs, files in os.walk(start):
            for name in files:
                if name.endswith(_META_SUFFIX) or name.startswith(".tmp-"):
                    continue
                key = (Path(current) / name).relative_to(self.root).as_posix()
                if key.startswith(prefix):
                    keys.append(key)
        return sorted(keys)

    def object_exists(self, key: str) -> bool:
        return self._path(key).is_file()
