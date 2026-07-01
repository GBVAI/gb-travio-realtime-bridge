"""Configuration loaded from environment / .env file.

All values are required at startup; we fail fast rather than degrading
silently, since silent degradation is how the real-time lane died
(gb-automations got 401s for weeks before anyone noticed).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path


def _load_env_file(path: Path) -> None:
    """Minimal .env loader (no external deps)."""
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


_load_env_file(Path("/etc/gb-travio-realtime-bridge.env"))
_load_env_file(Path.cwd() / ".env")


def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"FATAL: required env var {name} is not set", file=sys.stderr)
        sys.exit(2)
    return val


def _optional(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Config:
    # Travio API
    travio_base_url: str
    travio_id: int
    travio_key: str
    travio_requests_per_minute: int

    # Webhook receiver
    webhook_base_url: str
    webhook_admin_api_key: str
    webhook_poll_interval_seconds: int
    webhook_poll_limit: int
    webhook_state_db: str  # SQLite path for cursor persistence

    # Neon (gb-udb)
    neon_database_url: str

    # Operational
    log_level: str
    service_name: str
    dry_run: bool

    @classmethod
    def load(cls) -> "Config":
        return cls(
            travio_base_url=_optional("TRAVIO_BASE_URL", "https://api.travio.it/v2"),
            travio_id=int(_require("TRAVIO_ID")),
            travio_key=_require("TRAVIO_KEY"),
            travio_requests_per_minute=int(_optional("TRAVIO_REQUESTS_PER_MINUTE", "60")),
            webhook_base_url=_optional("WEBHOOK_BASE_URL", "https://travio.gbcrm.it"),
            webhook_admin_api_key=_require("WEBHOOK_ADMIN_API_KEY"),
            webhook_poll_interval_seconds=int(_optional("WEBHOOK_POLL_INTERVAL_SECONDS", "10")),
            webhook_poll_limit=int(_optional("WEBHOOK_POLL_LIMIT", "50")),
            webhook_state_db=_optional("WEBHOOK_STATE_DB", "/var/lib/gb-travio-realtime-bridge/state.db"),
            neon_database_url=_require("NEON_DATABASE_URL"),
            log_level=_optional("LOG_LEVEL", "info"),
            service_name=_optional("SERVICE_NAME", "gb-travio-realtime-bridge"),
            dry_run=_optional("DRY_RUN", "false").lower() in ("1", "true", "yes"),
        )
