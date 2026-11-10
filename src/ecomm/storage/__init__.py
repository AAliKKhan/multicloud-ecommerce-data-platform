"""Object storage interface and adapters."""

from ecomm.storage.interface import (
    ObjectNotFoundError,
    ObjectStore,
    StorageConfigError,
    StorageError,
    validate_key,
)
from ecomm.storage.local import LocalObjectStore

__all__ = [
    "LocalObjectStore",
    "ObjectNotFoundError",
    "ObjectStore",
    "StorageConfigError",
    "StorageError",
    "validate_key",
]
