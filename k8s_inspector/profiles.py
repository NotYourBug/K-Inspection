"""Finite profile definitions, shared without importing rule implementations."""
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True)
class ProfileSpec:
    name: str
    deadline_seconds: int
    schedule: str | None


PROFILES = MappingProxyType({p.name: p for p in (
    ProfileSpec('5m', 240, '*/5 * * * *'), ProfileSpec('10m', 480, '*/10 * * * *'),
    ProfileSpec('hourly', 900, '0 * * * *'), ProfileSpec('daily', 1800, '0 2 * * *'),
    ProfileSpec('full', 1800, None),
)})
ALIASES = {'dataplane-5m': '5m', 'dataplane': '5m', 'controlplane-10m': '10m',
           'controlplane': '10m', '1h': 'hourly', '1d': 'daily'}


def normalize_profile(value):
    if not isinstance(value, str):
        raise ValueError('profile must be a registered string')
    value = value.strip().lower()
    value = ALIASES.get(value, value)
    if value not in PROFILES:
        raise ValueError(f'unknown profile: {value}')
    return value
