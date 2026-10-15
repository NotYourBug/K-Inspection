"""Sanitized provider failure metadata shared by synchronous and queued sends."""
import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


class ProviderError(RuntimeError):
    def __init__(self, code=None, *, status=None, retry_after=None, unknown=False):
        # Provider messages/URLs may contain secrets. Keep only numeric codes.
        self.code = str(code) if type(code) is int else None
        self.status = status if type(status) is int else None
        self.retry_after = retry_after
        self.unknown = unknown
        super().__init__("Feishu delivery failed or was not confirmed")


def retry_after_seconds(headers) -> float | None:
    value = next((v for k, v in (headers or {}).items() if k.lower() == "retry-after"), None)
    if not isinstance(value, str):
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0.0, seconds) if math.isfinite(seconds) else None
