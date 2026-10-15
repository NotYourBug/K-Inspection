"""Single bounded delivery worker; external operations never hold a DB transaction."""
from __future__ import annotations

import logging
import math
import random
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable

from .bridge_store import DeliveryStore, StorageUnavailable
from .delivery_adapter import DeliveryAdapter
from .delivery_models import DeliveryResult

logger = logging.getLogger(__name__)


class DeliveryWorker:
    def __init__(self, store: DeliveryStore, adapter: DeliveryAdapter,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
                 retry_delays: tuple[int, ...] = (5, 15, 45, 120, 300), *,
                 jitter: Callable[[], float] = random.random, retry_deadline_seconds: int = 900):
        if len(retry_delays) != 5 or any(not math.isfinite(v) or v <= 0 for v in retry_delays):
            raise ValueError("five positive finite retry delays are required")
        if type(retry_deadline_seconds) is not int or retry_deadline_seconds <= 0:
            raise ValueError("retry deadline must be positive")
        self.store, self.adapter, self.clock = store, adapter, clock
        self.retry_delays, self.jitter = retry_delays, jitter
        self.retry_deadline_seconds = retry_deadline_seconds
        self.last_heartbeat: datetime | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> bool:
        self.last_heartbeat = self.clock()
        if self._stop.is_set():
            return False
        job = self.store.claim_due(self.clock())
        if job is None:
            return False
        if self._stop.is_set():
            self.store.release_claim(job.delivery_id, job.lease_token, self.clock())
            return False
        deadline = job.budget_started_at + timedelta(seconds=self.retry_deadline_seconds)
        if job.attempt_count > 1 and self.clock() >= deadline:
            result = DeliveryResult(False, error_kind="retry_deadline")
        else:
            try:
                result = self.adapter.send(job)
            except Exception:
                result = DeliveryResult(False, True, True, error_kind="provider_unknown")
        now = self.clock()
        retry_at = None
        if not result.success and result.retryable and job.attempt_count <= 5:
            delay = self.retry_delays[job.attempt_count - 1] * (1 + 0.1 * min(1, max(0, self.jitter())))
            if result.retry_after_seconds is not None and math.isfinite(result.retry_after_seconds):
                delay = max(delay, result.retry_after_seconds)
            if (deadline - now).total_seconds() >= delay:
                retry_at = now + timedelta(seconds=delay)
        try:
            self.store.finish(job.delivery_id, job.lease_token, result, now, retry_at=retry_at)
        except StorageUnavailable:
            # Leave the SENDING lease intact: restart recovery records uncertainty.
            logger.error("delivery result could not be committed; lease recovery required")
        self.last_heartbeat = self.clock()
        return True

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="bridge-delivery", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    processed = self.run_once()
                except StorageUnavailable:
                    logger.error("delivery storage unavailable")
                    processed = False
                if not processed:
                    self._stop.wait(0.5)
        finally:
            self.last_heartbeat = None

    def stop(self, grace_seconds: float = 15) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(max(0, grace_seconds))
