"""One confirmed provider operation. Retry scheduling belongs to the worker."""
from __future__ import annotations

import copy
from collections.abc import Mapping
from urllib.error import HTTPError, URLError

from requests.exceptions import ConnectionError as RequestsConnectionError, Timeout

from .delivery_models import DeliveryJob, DeliveryResult
from .feishu_app import FeishuAppClient
from .notify import send_feishu_text
from .provider_error import ProviderError, retry_after_seconds

# Official IM and generic code references are recorded in the delivery runbook.
# Unknown business codes get the same bounded retry budget, never infinite retry.
PERMANENT_CODES = {230001, 230002, 230006, 230013, 99991401, 99991403,
                   99991543, 99991662, 99991672, 99991673, 99991676}
TRANSIENT_CODES = {99991400, 11247, 1500, 1503, 1551}


class DeliveryAdapter:
    def __init__(self, *, feishu_app: FeishuAppClient | None = None,
                 webhooks: Mapping[str, str] | None = None):
        self.feishu_app = feishu_app
        self.webhooks = dict(webhooks or {})

    def send(self, job: DeliveryJob) -> DeliveryResult:
        try:
            if job.payload.get("mode") == "app":
                if self.feishu_app is None:
                    return DeliveryResult(False, error_kind="configuration")
                card = copy.deepcopy(job.payload["card"])
                card.setdefault("elements", []).append({"tag": "div", "text": {
                    "tag": "plain_text", "content": f"delivery_id: {job.delivery_id}"}})
                external_id = self.feishu_app.send_card_message(
                    job.payload["chat_id"], card, message_uuid=job.delivery_id)
                return DeliveryResult(True, external_id=external_id)
            if job.payload.get("mode") == "webhook":
                url = self.webhooks.get(job.destination_id)
                if not url:
                    return DeliveryResult(False, error_kind="configuration")
                send_feishu_text(url, f"{job.payload['text']}\ndelivery_id: {job.delivery_id}", timeout_s=10)
                return DeliveryResult(True)
            return DeliveryResult(False, error_kind="configuration")
        except ProviderError as exc:
            code = int(exc.code) if exc.code is not None else None
            retryable = (exc.status == 429 or (exc.status is not None and exc.status >= 500)
                         or code in TRANSIENT_CODES or
                         (code not in PERMANENT_CODES and (exc.status is None or exc.status < 400)))
            return DeliveryResult(False, retryable, exc.unknown, error_code=exc.code,
                retry_after_seconds=exc.retry_after,
                error_kind="provider_transient" if retryable else "provider_permanent")
        except HTTPError as exc:
            return DeliveryResult(False, exc.code == 429 or exc.code >= 500,
                error_code=str(exc.code), retry_after_seconds=retry_after_seconds(exc.headers),
                error_kind="http")
        except (TimeoutError, Timeout):
            return DeliveryResult(False, True, True, error_kind="timeout")
        except (URLError, OSError, RequestsConnectionError):
            return DeliveryResult(False, True, True, error_kind="transport")
        except Exception as exc:
            code = getattr(exc, "code", None)
            if type(code) is int and code in PERMANENT_CODES | TRANSIENT_CODES:
                retryable = code in TRANSIENT_CODES
                return DeliveryResult(False, retryable, error_code=str(code),
                    error_kind="provider_transient" if retryable else "provider_permanent")
            # Includes SDK token/unmarshal exceptions; never stringify provider data.
            return DeliveryResult(False, True, True, error_kind="provider_unknown")
