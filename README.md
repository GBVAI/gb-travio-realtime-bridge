# GB Travio Realtime Bridge

Closes the gap caused by `gb-automations`'s IP-allowlist failure on Railway:
reads events from `gb-travio-webhooks` (Railway, healthy) and fetches the
full record from Travio's REST API directly from this host (which has a
Travio-allowlisted egress IP). Writes the result into the same `gb-udb`
(Neon Postgres) tables that `gb-automations` was writing to before the
incident.

## Why this exists

| Component | Status | Notes |
|-----------|--------|-------|
| `gb-travio-webhooks` (Railway) | ✅ healthy | Receives Travio webhooks durably |
| `gb-automations` (Railway) | ❌ dead since ~June 5, 2026 | Outbound to Travio: 401 (IP allowlist) |
| `gb-travioetl/neon_bulk_load.py` (this host, daily) | ✅ running | Nightly full-mirror from Travio's own MySQL backup |
| `gb-travio-realtime-bridge` (this host, new) | **👈 this service** | Polls webhook receiver + calls Travio directly from this host |

The nightly mirror gives us a fresh baseline every morning. This bridge
keeps intraday updates current in `gb-udb` so consumers like `gb-mari` can
show status changes, new bookings, and cancellations in real time.

## Architecture

```
Travio ──webhooks──► gb-travio-webhooks (Railway)
                          │ GET /internal/events
                          ▼
                    [this service] ──HTTPS──► Travio /v2/rest/...  (allowlisted IP)
                          │
                          ▼
                    gb-udb (Neon)  — reservations, master_data, pax, services, history, snapshots
                          ▲
                          │ nightly full-mirror UPSERT (with freshness guard)
                    gb-travioetl/neon_bulk_load.py
```

## How it works

1. Every `WEBHOOK_POLL_INTERVAL_SECONDS` (default 10s), query the webhook
   receiver's `/internal/events` for events newer than our cursor.
2. For each event, parse `event_subject` (`reservations:<id>` or
   `master-data:<id>`) and fetch the full record from Travio's REST API
   with all linked fields and sublists expanded.
3. Upsert into the corresponding Neon table (and child tables for
   contacts/addresses/pax/services/status history).
4. Snapshot to `*_snapshots` only when the SHA-256 of the payload changes.
5. Enqueue CRM sync via `crm_sync_state` (consumed by `push-to-crm` job).
6. Advance cursor to the event id we successfully processed.

Crash safety: the cursor is persisted to SQLite (in
`/var/lib/gb-travio-realtime-bridge/state.db`) **after** each successful
event. A crash mid-batch leaves the cursor at the last successful event;
events ahead of the cursor are retried on the next tick. The receiver
itself stores every event durably so this is safe to retry indefinitely.

## Configuration

All settings come from environment variables (or a `.env` file in cwd).
See `.env.example` for the full list.

| Var | Required | Default | Notes |
|-----|----------|---------|-------|
| `TRAVIO_ID` | yes | — | Travio API client id |
| `TRAVIO_KEY` | yes | — | Travio API client key |
| `WEBHOOK_ADMIN_API_KEY` | yes | — | Bearer token for `gb-travio-webhooks` internal API |
| `NEON_DATABASE_URL` | yes | — | Postgres DSN for `gb-udb` |
| `TRAVIO_BASE_URL` | no | `https://api.travio.it/v2` | |
| `WEBHOOK_BASE_URL` | no | `https://travio.gbcrm.it` | |
| `WEBHOOK_POLL_INTERVAL_SECONDS` | no | `10` | |
| `WEBHOOK_POLL_LIMIT` | no | `50` | Events per poll |
| `TRAVIO_REQUESTS_PER_MINUTE` | no | `60` | Throttle (token bucket) |
| `WEBHOOK_STATE_DB` | no | `/var/lib/.../state.db` | SQLite cursor store |
| `LOG_LEVEL` | no | `info` | `debug` / `info` / `warn` / `error` |
| `DRY_RUN` | no | `false` | If true, fetch but don't write |

## Run

```bash
# Local dev
python3 -m src.main

# With env file
cp .env.example /etc/gb-travio-realtime-bridge.env
chmod 600 /etc/gb-travio-realtime-bridge.env
# (edit then start the systemd unit)
sudo systemctl start gb-travio-realtime-bridge
```

## On-demand single-record refresh

For ops use, when someone spots a stale row:

```bash
# Refresh one reservation (fetches from Travio, upserts to gb-udb)
PYTHONPATH=src python3 -m src.refresh_one reservation 1107373

# Refresh one master_data record
PYTHONPATH=src python3 -m src.refresh_one master_data 412156

# Dry-run (fetch but don't write) + JSON output for scripting
PYTHONPATH=src python3 -m src.refresh_one reservation 1107373 --dry-run --json
```

Exit codes are scriptable: `0` success, `2` bad args, `3` Travio fetch error,
`4` record not found in Travio, `5` Neon write error.

Example usage in a triage script:

```bash
if PYTHONPATH=src python3 -m src.refresh_one reservation "$ID" --json | jq -e '.action == "created" or .action == "updated"'; then
  echo "Refreshed reservation $ID"
fi
```

## Date-range backfill

For catching up a range of dates (e.g. after a long bridge outage, or
specifically to refresh the 02:49-06:xx window between Travio's backup
and the nightly mirror):

```bash
# Backfill everything booked 2026-07-01
PYTHONPATH=src python3 -m src.backfill --since=2026-07-01 --until=2026-07-01

# Multi-day window with higher concurrency
PYTHONPATH=src python3 -m src.backfill --since=2026-06-29 --until=2026-07-01 \
    --concurrency=8 --per-page=500

# Explicit list of IDs (skips the page-walk discovery)
PYTHONPATH=src python3 -m src.backfill --ids=1107373,1107374,1107375

# Dry-run to see what would be touched
PYTHONPATH=src python3 -m src.backfill --since=2026-07-01 --until=2026-07-01 \
    --dry-run --json
```

**Why page-walk instead of date-filter?** Travio's `filters=` and `sort_by=`
query parameters are currently broken server-side (every value shape
returns HTTP 500 with "filters.push is not a function" or "Bad sort by
format"). The only working navigation primitive is `page` + `per_page` in
default (ascending-by-id) order. The backfill walks backwards from the last
page and stops as soon as it crosses below `since`. The `date` field is
roughly monotonic with id (page 2200 = 2026-07-01, page 2000 = 2026-01,
page 1000 = 2019), so the walk converges fast: a 1-day backfill scans
~3 pages and finishes in ~30s of discovery + N minutes of per-id re-fetch
depending on `TRAVIO_REQUESTS_PER_MINUTE`.

Exit codes: `0` success, `2` bad args, `3` Travio fetch error, `5` Neon
write error (any failures).

## Operational notes

- **No public port required.** This service is an outbound-only client.
  It does NOT accept any inbound HTTP. The webhook receiver is public;
  we just poll it.
- **This host must be Travio-allowlisted.** Run the smoke test before
  deploying — Railway's IP is NOT allowlisted, so do not run this from
  Railway.
- **Both this service and the nightly mirror write to the same tables.**
  The freshness guard in `neon_bulk_load.py` (commit `64cc865`,
  `gb-travioetl` branch `neon-sync`) prevents the mirror from clobbering
  intraday updates; it only overwrites when `travio_last_update` is `>=`
  the stored value.
- **Don't forget the systemd unit.** See `deploy/gb-travio-realtime-bridge.service`.
