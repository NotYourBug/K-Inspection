"""Persisted delivery contracts; timestamps at the boundary are timezone aware."""
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class DeliveryTarget:
    destination_id: str
    content_hash: str
    payload: dict[str, object]


@dataclass(frozen=True)
class AcceptResult:
    receipt_id: str
    new_job_ids: tuple[str, ...]
    reused_job_ids: tuple[str, ...]


@dataclass(frozen=True)
class DeliveryJob:
    delivery_id: str
    source_id: str
    destination_id: str
    content_hash: str
    generation: int
    payload: dict[str, object]
    state: str
    attempt_count: int
    next_attempt_at: datetime
    lease_token: str | None
    lease_until: datetime | None
    external_id: str | None
    error_kind: str | None
    created_at: datetime
    sent_at: datetime | None = None
    result_unknown: bool = False
    error_code: str | None = None


@dataclass(frozen=True)
class DeliveryResult:
    success: bool
    retryable: bool = False
    result_unknown: bool = False
    external_id: str | None = None
    error_code: str | None = None
    retry_after_seconds: float | None = None
    error_kind: str | None = None
