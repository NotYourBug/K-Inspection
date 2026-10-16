"""Read-only per-run collection snapshot."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Mapping, TypeVar
from types import MappingProxyType
from .observation_models import ExecutionStatus, utc

T = TypeVar('T')


@dataclass(frozen=True)
class Collection(Generic[T]):
    items: tuple[T, ...]
    execution_status: ExecutionStatus
    observed_at: datetime
    complete: bool
    error_kind: str | None = None
    reason: str | None = None

    def __post_init__(self):
        object.__setattr__(self, 'observed_at', utc(self.observed_at))


@dataclass(frozen=True)
class RunSnapshot:
    run_id: str
    cluster_id: str
    started_at: datetime
    collections: Mapping[str, Collection[object]]

    def __post_init__(self):
        object.__setattr__(self, 'started_at', utc(self.started_at))
        object.__setattr__(self, 'collections', MappingProxyType(dict(self.collections)))
