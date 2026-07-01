"""Client for the gb-travio-webhooks internal event API on Railway.

We poll this instead of having the webhook receiver forward events to us,
because:
- this host has no public IP / no inbound port exposed
- the receiver already stores every event durably, so we can re-poll
  any that we missed
- the cursor API gives at-least-once delivery semantics for free

The auth is the receiver's ADMIN_API_KEY passed as a Bearer token.
"""

from __future__ import annotations

import json
import time
from typing import Any
from urllib import error, parse, request


class WebhookReceiverError(Exception):
    pass


class WebhookReceiverClient:
    def __init__(self, base_url: str, admin_api_key: str, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.admin_api_key = admin_api_key
        self.timeout = timeout

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        if params:
            url += "?" + parse.urlencode(params)
        req = request.Request(url, headers={
            "Authorization": f"Bearer {self.admin_api_key}",
            "Accept": "application/json",
        })
        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            raise WebhookReceiverError(f"HTTP {e.code} {raw[:200]}") from e
        except (error.URLError, TimeoutError, OSError) as e:
            raise WebhookReceiverError(f"network error: {e}") from e

    def health(self) -> dict:
        return self._get("/internal/runtime")

    def list_events(self, *, after_id: int, limit: int = 50) -> dict:
        """List events with id < after_id, descending (so newest first).

        The receiver orders by (first_received_at desc, id desc) and uses
        cursor pagination; for our use case 'give me everything after this id'
        is the natural query and we just paginate by id ourselves to keep the
        state machine dead simple.

        NOTE: The receiver cursor encodes (first_received_at, id). If two
        events arrive in the same millisecond with different ids, ordering
        by id desc still gives a strict total order. If they have the same
        id (impossible — id is a serial), we'd miss one. Safe to ignore.
        """
        # We can't use the cursor format directly because the cursor encodes
        # BOTH first_received_at AND id, and we want to advance by id alone.
        # So we filter client-side from a broad query and only use the cursor
        # for pagination. The receiver sorts newest-first so we walk backwards.
        all_events: list[dict] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": str(limit)}
            if cursor:
                params["cursor"] = cursor
            page = self._get("/internal/events", params)
            events = page.get("events", [])
            if not events:
                break
            # Filter to events strictly older than our last-seen id
            new_events = [e for e in events if int(e["id"]) < after_id]
            all_events.extend(new_events)
            # If the oldest event in this page is still newer than our
            # last-seen id, keep paginating forward in time.
            oldest_id = int(events[-1]["id"])
            if oldest_id >= after_id and page.get("hasMore"):
                cursor = page.get("nextCursor")
                if not cursor:
                    break
                continue
            break
        return {
            "events": all_events,
            "newest_id": max((int(e["id"]) for e in all_events), default=after_id),
        }
