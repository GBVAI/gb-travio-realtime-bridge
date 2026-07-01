"""Minimal JSON-line logger. We emit one JSON object per line on stdout so a
journald or file-collector can pick it up cleanly; this also matches the
shape gb-travio-proxy already produces."""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any


_LEVELS = {"debug": 10, "info": 20, "warn": 30, "error": 40}


class Logger:
    def __init__(self, service: str, level: str = "info") -> None:
        self.service = service
        self.threshold = _LEVELS.get(level.lower(), 20)
        self.hostname = os.uname().nodename

    def _emit(self, level: str, message: str, **fields: Any) -> None:
        if _LEVELS[level] < self.threshold:
            return
        rec = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int((time.time()%1)*1000):03d}Z",
            "level": level,
            "service": self.service,
            "hostname": self.hostname,
            "message": message,
            **fields,
        }
        sys.stdout.write(json.dumps(rec, default=str) + "\n")
        sys.stdout.flush()

    def debug(self, msg: str, **kw: Any) -> None: self._emit("debug", msg, **kw)
    def info(self, msg: str, **kw: Any) -> None:  self._emit("info", msg, **kw)
    def warn(self, msg: str, **kw: Any) -> None:  self._emit("warn", msg, **kw)
    def error(self, msg: str, **kw: Any) -> None: self._emit("error", msg, **kw)
