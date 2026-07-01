"""Persistent state for the bridge. Currently only the 'last seen event id'
from the webhook receiver; this lets us resume after a restart without
re-processing the entire history.

Using SQLite (stdlib) rather than a separate Postgres state table to keep
operational dependencies minimal — this service already depends on the
gb-udb Postgres for the actual data writes.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path


_SCHEMA = """
CREATE TABLE IF NOT EXISTS state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, isolation_level=None)

    def get(self, key: str, default: str | None = None) -> str | None:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
            return row[0] if row else default

    def set(self, key: str, value: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO state (key, value) VALUES (?, ?) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (key, value),
            )

    def get_int(self, key: str, default: int) -> int:
        v = self.get(key)
        if v is None:
            return default
        try:
            return int(v)
        except ValueError:
            return default

    def set_int(self, key: str, value: int) -> None:
        self.set(key, str(value))
