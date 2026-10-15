# Feishu delivery adapter evidence

Offline verified runtime: Python 3.12.14, lark-oapi **1.7.5** (pinned in both install sources).
The installed `ClientBuilder.timeout()` sets `Config.timeout`; `Transport.execute`
passes it to `requests.request(timeout=...)`, including token acquisition. This is
a socket connect/read timeout per HTTP operation, not an end-to-end cancellation
deadline. Token acquisition and message creation can use separate operations.
The installed `CreateMessageRequestBodyBuilder.uuid()` receives the persisted UUID.

The adapter treats HTTP 429 and 5xx as transient, other 4xx as permanent. Business
codes 99991400/11247 and 1500/1503/1551 are transient; 99991401/99991403,
99991543, 99991662/99991672/99991673/99991676 require configuration or permission
repair. These meanings come from the [official generic error table](https://open.feishu.cn/document/server-docs/api-call-guide/generic-error-code).

For IM errors 230001/230002/230006/230013, parameter, membership, bot capability and
availability failures are treated as permanent. The [official IM error table](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/list)
documents these meanings; applying them to the sending adapter is an explicit
IM-family inference. A real send contract test remains an E5 release gate.

Unknown business codes receive bounded retries and remain visible as their numeric
code. Unconfirmed transport/invalid response results retain an unknown flag. Raw
provider messages, bodies, URLs and credentials are never stored as error details.
An HTTP 200 alone cannot confirm business success.

[Send-message API](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/create)
and [official UUID builder source](https://github.com/larksuite/oapi-sdk-python/blob/v1.7.3/lark_oapi/api/im/v1/model/create_message_request_body.py)
are reference contracts. The real provider's UUID window and behavior after
ambiguous responses have **not** been measured. Delivery remains at least once;
webhook retries can duplicate messages. No real notification was sent in this batch.
