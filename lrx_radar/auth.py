from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from threading import Lock
from time import monotonic
from typing import Iterable

from fastapi import Header, HTTPException, status

from lrx_radar.schemas import RadarScanIn


ROLE_READ = "read"
ROLE_INGEST = "ingest"
ROLE_ADMIN = "admin"


@dataclass(frozen=True, slots=True)
class Principal:
    api_key: str
    role: str
    source_scopes: tuple[str, ...]

    @property
    def is_admin(self) -> bool:
        return self.role == ROLE_ADMIN


class AuthManager:
    """
    API-key authentication with role and source scoping.

    API key config format:
      KEY:ROLE:SCOPE
    Examples:
      dev-admin-key:admin:*
      read-key:read:station-alpha|station-beta
      ingest-key:ingest:station-
    Entries are comma-separated.
    """

    def __init__(self, key_config: str | None = None) -> None:
        config = key_config or "dev-admin-key:admin:*"
        self._principals: dict[str, Principal] = {}
        for token in config.split(","):
            entry = token.strip()
            if not entry:
                continue
            parts = entry.split(":", maxsplit=2)
            if len(parts) < 2:
                continue
            api_key = parts[0].strip()
            role = parts[1].strip().lower()
            scope_raw = parts[2].strip() if len(parts) > 2 else "*"
            scopes = tuple(
                scoped.strip()
                for scoped in scope_raw.replace(";", "|").split("|")
                if scoped.strip()
            )
            if not scopes:
                scopes = ("*",)
            if role not in {ROLE_READ, ROLE_INGEST, ROLE_ADMIN}:
                continue
            self._principals[api_key] = Principal(api_key=api_key, role=role, source_scopes=scopes)

        if not self._principals:
            self._principals["dev-admin-key"] = Principal(
                api_key="dev-admin-key", role=ROLE_ADMIN, source_scopes=("*",)
            )

    def authenticate(self, api_key: str | None) -> Principal:
        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing API key. Provide x-api-key header.",
            )
        principal = self._principals.get(api_key)
        if principal is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key.",
            )
        return principal

    def authorize(self, principal: Principal, allowed_roles: Iterable[str]) -> None:
        roles = set(allowed_roles)
        if principal.role in roles or principal.is_admin:
            return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions for this operation.",
        )

    def enforce_source_scope(self, principal: Principal, source_id: str) -> None:
        if principal.is_admin:
            return
        if "*" in principal.source_scopes:
            return
        if any(source_id.startswith(scope) for scope in principal.source_scopes):
            return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"API key is not allowed to access source '{source_id}'.",
        )


class SourceQuotaManager:
    """Per-source per-key fixed-window quota guard."""

    def __init__(
        self,
        *,
        default_per_minute: int = 2_000,
        per_source_overrides: dict[str, int] | None = None,
    ) -> None:
        self.default_per_minute = max(1, default_per_minute)
        self.per_source_overrides = per_source_overrides or {}
        self._events: dict[tuple[str, str], deque[float]] = {}
        self._lock = Lock()

    def _quota_for_source(self, source_id: str) -> int:
        return max(1, self.per_source_overrides.get(source_id, self.default_per_minute))

    def consume(self, principal: Principal, scans: list[RadarScanIn]) -> None:
        per_source_counts = Counter(scan.source_id for scan in scans)
        now = monotonic()
        cutoff = now - 60.0
        with self._lock:
            for source_id, requested in per_source_counts.items():
                bucket_key = (principal.api_key, source_id)
                history = self._events.setdefault(bucket_key, deque())
                while history and history[0] < cutoff:
                    history.popleft()
                quota = self._quota_for_source(source_id)
                if len(history) + requested > quota:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=(
                            f"Quota exceeded for source '{source_id}'. "
                            f"Requested={requested}, available={max(0, quota - len(history))}, "
                            f"limit_per_minute={quota}."
                        ),
                    )
            for source_id, requested in per_source_counts.items():
                history = self._events[(principal.api_key, source_id)]
                history.extend([now] * requested)


def parse_source_quota_overrides(raw: str | None) -> dict[str, int]:
    if not raw:
        return {}
    parsed: dict[str, int] = {}
    for token in raw.split(","):
        entry = token.strip()
        if "=" not in entry:
            continue
        source_id, limit = entry.split("=", maxsplit=1)
        source = source_id.strip()
        if not source:
            continue
        try:
            parsed[source] = max(1, int(limit.strip()))
        except ValueError:
            continue
    return parsed


def api_key_from_header(x_api_key: str | None = Header(default=None)) -> str | None:
    return x_api_key
