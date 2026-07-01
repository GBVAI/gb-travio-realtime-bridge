"""Travio REST API client. Direct calls (no proxy) — only works from an
allowlisted egress IP, which this host has (Railway does not).

The shape mirrors `gbbookingincentives/automations/src/lib/travio.ts` so a
reader who knows that file can navigate this one.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request


# Travio returns 'id' values as integers in JSON; we treat them as int
# everywhere and only convert to string when they're used as dict keys.

# NOTE: Travio's actual API uses these status IDs as of 2026-07:
#   3 = Confermata (Confirmed), 6 = In attesa (Waiting)
# This differs from the older mapping in some docs. The status_name
# returned by the API takes precedence; this map is only used as
# fallback when status_history entries contain only an int.
TRAVIO_STATUS_NAMES: dict[int, str] = {
    0: "Preventivo",   # Quote
    1: "Inserita",     # Entered
    2: "Rifiutata",    # Rejected
    3: "Confermata",   # Confirmed (modern Travio; older docs said "Parziale")
    4: "Completata",   # Completed
    5: "Annullata",    # Cancelled
    6: "In attesa",    # Waiting
}

INTERNATIONAL_KEYWORDS = ("mondo", "international", "world", "europa",
                          "america", "asia", "africa", "oceania")
INSURANCE_KEYWORDS = ("assicur", "insurance")

# Standard unfold params from the TS client; Travio needs the linked fields
# expanded to include human-readable names + sublists for nested data.
RESERVATION_UNFOLD = "heading,status,client,invoice_client,payment_client,promoter,network,user"
RESERVATION_UNFOLD_SUBLISTS = "services,pax,status_history,accounting_entries,instalments"
MASTER_DATA_UNFOLD = "contacts,addresses,invoice_master_data,inbound_payments_master_data,outbound_payments_master_data,promoter,network,legal_form,honorific,categories,price_lists,profile_type"


class TravioError(Exception):
    """Raised on non-recoverable Travio API errors."""


@dataclass
class TravioAuth:
    token: str
    expires_at: float  # unix seconds


class TravioClient:
    def __init__(self, base_url: str, client_id: int, client_key: str,
                 requests_per_minute: int = 60, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id
        self.client_key = client_key
        self.timeout = timeout
        # Token-bucket throttle to respect requests_per_minute
        self._interval = 60.0 / max(1, requests_per_minute)
        self._last_request = 0.0
        self._lock = threading.Lock()
        self._auth: TravioAuth | None = None

    # ─── throttling ──────────────────────────────────────────────────────
    def _throttle(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._interval - (now - self._last_request)
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

    # ─── http helpers ────────────────────────────────────────────────────
    def _request(self, method: str, path: str, body: dict | None = None,
                 extra_headers: dict | None = None) -> tuple[int, dict | str]:
        self._throttle()
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        else:
            data = None
        if extra_headers:
            headers.update(extra_headers)
        req = request.Request(url, data=data, method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                try:
                    return resp.status, json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    return resp.status, raw
        except error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            try:
                return e.code, json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                return e.code, raw
        except (error.URLError, TimeoutError, OSError) as e:
            raise TravioError(f"network error: {e}") from e

    # ─── auth ────────────────────────────────────────────────────────────
    def _ensure_auth(self) -> str:
        if self._auth and self._auth.expires_at - time.time() > 30:
            return self._auth.token
        status, body = self._request("POST", "/auth",
                                     body={"id": self.client_id, "key": self.client_key})
        if status != 200 or not isinstance(body, dict) or "token" not in body:
            raise TravioError(f"auth failed: HTTP {status} body={body!r}")
        token = body["token"]
        # Travio tokens are 8h; we refresh every 55min to stay well under.
        self._auth = TravioAuth(token=token, expires_at=time.time() + 55 * 60)
        return token

    def _authed(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict | str]:
        status: int = 0
        resp: dict | str = {}
        for attempt in range(3):
            token = self._ensure_auth()
            status, resp = self._request(method, path, body=body,
                                         extra_headers={"Authorization": f"Bearer {token}"})
            if status == 401 and attempt < 2:
                # Token likely expired; force refresh next attempt
                self._auth = None
                continue
            return status, resp
        return status, resp

    # ─── reservations ────────────────────────────────────────────────────
    def get_reservation(self, travio_id: int) -> dict | None:
        path = f"/rest/reservations/{travio_id}?unfold={RESERVATION_UNFOLD}&unfold_sublists={RESERVATION_UNFOLD_SUBLISTS}"
        status, body = self._authed("GET", path)
        if status == 404:
            return None
        if status != 200 or not isinstance(body, dict):
            raise TravioError(f"get_reservation({travio_id}): HTTP {status} body={body!r}")
        # Travio wraps in { data: ... } for single-record endpoints
        return body.get("data", body) if isinstance(body, dict) else None

    def list_reservations_page(self, page: int, per_page: int = 100,
                               sort_by: list[dict] | None = None,
                               filters: list[dict] | None = None) -> dict:
        params: list[tuple[str, str]] = [("page", str(page)), ("per_page", str(per_page))]
        if sort_by:
            params.append(("sort_by", json.dumps(sort_by)))
        if filters:
            params.append(("filter", json.dumps(filters)))
        path = "/rest/reservations?" + parse.urlencode(params)
        status, body = self._authed("GET", path)
        if status != 200 or not isinstance(body, dict):
            raise TravioError(f"list_reservations: HTTP {status} body={body!r}")
        return body

    # ─── master data ─────────────────────────────────────────────────────
    def get_master_data(self, travio_id: int) -> dict | None:
        path = f"/rest/master-data/{travio_id}?unfold={MASTER_DATA_UNFOLD}"
        status, body = self._authed("GET", path)
        if status == 404:
            return None
        if status != 200 or not isinstance(body, dict):
            raise TravioError(f"get_master_data({travio_id}): HTTP {status} body={body!r}")
        return body.get("data", body) if isinstance(body, dict) else None

    def list_master_data_page(self, page: int, per_page: int = 100) -> dict:
        path = f"/rest/master-data?page={page}&per_page={per_page}"
        status, body = self._authed("GET", path)
        if status != 200 or not isinstance(body, dict):
            raise TravioError(f"list_master_data: HTTP {status} body={body!r}")
        return body

    # ─── helpers exposed for the writer layer ────────────────────────────
    @staticmethod
    def link_id(field: Any) -> int | None:
        """Like neon-writer.ts linkId: Travio may return either an int or
        an object {id, ...} depending on whether it was unfolded. Always
        return the int, or None."""
        if field is None:
            return None
        if isinstance(field, int):
            return field
        if isinstance(field, dict) and "id" in field:
            try:
                return int(field["id"])
            except (TypeError, ValueError):
                return None
        return None

    @staticmethod
    def status_info(status: Any) -> tuple[int | None, str | None]:
        """Like extractStatusInfo: returns (id, name)."""
        if status is None:
            return None, None
        if isinstance(status, int):
            return status, TRAVIO_STATUS_NAMES.get(status)
        if isinstance(status, dict) and "id" in status:
            try:
                sid = int(status["id"])
            except (TypeError, ValueError):
                return None, None
            name_field = status.get("name")
            if isinstance(name_field, str):
                return sid, name_field
            if isinstance(name_field, dict):
                return sid, name_field.get("en") or name_field.get("it")
            return sid, TRAVIO_STATUS_NAMES.get(sid)
        return None, None

    @staticmethod
    def is_international(reservation: dict) -> bool:
        for service in reservation.get("services") or []:
            name = (service.get("name") or "").lower()
            if any(kw in name for kw in INTERNATIONAL_KEYWORDS):
                return True
        return False

    @staticmethod
    def has_insurance(reservation: dict) -> tuple[bool, float | None]:
        found = False
        margin: float | None = None
        for service in reservation.get("services") or []:
            name = (service.get("name") or "").lower()
            if any(kw in name for kw in INSURANCE_KEYWORDS):
                found = True
                for row in service.get("rows") or []:
                    price_g = (row.get("price") or {}).get("gross") or 0
                    cost_g = (row.get("cost") or {}).get("gross") or 0
                    margin = (margin or 0) + (float(price_g) - float(cost_g))
        return found, margin
