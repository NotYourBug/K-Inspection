"""Credential-bound logical sources; body fields never choose the trusted source."""
import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class SourceCredential:
    source_id: str
    token: str = field(repr=False)


class AuthenticationError(ValueError):
    pass


def validate_sources(sources: tuple[SourceCredential, ...]) -> None:
    if len(sources) > 64:
        raise ValueError("too many source credentials")
    tokens = set()
    for source in sources:
        if (not isinstance(source.source_id, str) or
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", source.source_id)):
            raise ValueError("invalid logical source ID")
        if (not isinstance(source.token, str) or not source.token.isascii() or
                not 32 <= len(source.token) <= 512 or any(c.isspace() for c in source.token)):
            raise ValueError("source token must be 32–512 non-whitespace ASCII characters")
        if source.token in tokens:
            raise ValueError("duplicate source credential")
        tokens.add(source.token)


def load_sources(path: str) -> tuple[SourceCredential, ...]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError
        sources = tuple(SourceCredential(**item) for item in data)
        validate_sources(sources)
        return sources
    except (ValueError, TypeError, OSError) as exc:
        raise ValueError("invalid source credentials file") from exc


def authenticate(headers: Mapping[str, str], sources: tuple[SourceCredential, ...]) -> str:
    value = next((v for k, v in headers.items() if k.lower() == "authorization"), "")
    scheme, _, token = value.partition(" ")
    if scheme.lower() != "bearer" or not token or len(token) > 512:
        raise AuthenticationError("authentication required")
    digest = hashlib.sha256(token.encode("utf-8")).digest()
    matched = None
    for source in sources:
        if hmac.compare_digest(digest, hashlib.sha256(source.token.encode("utf-8")).digest()):
            matched = source.source_id
    if matched is None:
        raise AuthenticationError("authentication required")
    return matched
