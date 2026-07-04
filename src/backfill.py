"""Date-range or explicit-IDs backfill for the realtime bridge.

Travio's `filters=` and `sort_by=` query parameters are currently broken
server-side (every value shape returns HTTP 500 with "filters.push is not a
function" or "Bad sort by format"). The only working navigation primitive
is `page` + `per_page` in default (ascending-by-id) order.

This tool implements the same pagination strategy as
`gbbookingincentives/automations/src/jobs/smart-sync.ts`:

  1. If `--ids` is given, skip discovery and re-fetch each ID directly.
  2. Otherwise, page-walk backwards from the last page (highest/default-latest
     page in ascending order), keeping IDs whose `date` field falls in the
     requested [since, until] range. Stop once we cross below `since`.
  3. For each collected ID, re-fetch the full record via get_reservation
     and upsert into Neon.

Usage:
    python3 -m src.backfill --since=2026-06-29 --until=2026-07-01
    python3 -m src.backfill --since=2026-06-29 --until=2026-07-01 --concurrency=8
    python3 -m src.backfill --ids=1107373,1110180,1107374
    python3 -m src.backfill --since=2026-06-29 --until=2026-07-01 --dry-run --json

Exit codes:
    0  success
    2  bad CLI args
    3  Travio fetch failed (at the discovery or per-id stage)
    5  Neon write failed
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# Make src/ importable when run as `python3 -m src.backfill` from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import Config  # noqa: E402
from logging_setup import Logger  # noqa: E402
from travio_client import TravioClient, TravioError  # noqa: E402
from neon_writer import NeonWriter  # noqa: E402

EMPTY_PAGE_STOP_THRESHOLD = 5  # safety net; main early-exit is the date-boundary check


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill reservations from Travio into gb-udb using pagination",
    )
    p.add_argument("--since", help="Lower bound of booking date (YYYY-MM-DD), inclusive")
    p.add_argument("--until", help="Upper bound of booking date (YYYY-MM-DD), inclusive")
    p.add_argument("--ids", help="Comma-separated list of explicit travio_ids to refresh")
    p.add_argument("--concurrency", type=int, default=4,
                   help="Number of parallel per-id re-fetches (default 4)")
    p.add_argument("--per-page", type=int, default=500,
                   help="Page size for the discovery walk (default 500, max 500)")
    p.add_argument("--dry-run", action="store_true",
                   help="Discover and fetch from Travio but do not write to Neon")
    p.add_argument("--json", action="store_true",
                   help="Emit a single JSON object as the final output line")
    p.add_argument("--max-ids", type=int, default=0,
                   help="If > 0, cap the number of IDs collected (safety valve)")
    args = p.parse_args()

    if not args.ids and not (args.since and args.until):
        p.error("either --ids OR (--since AND --until) is required")

    if args.ids:
        bad_ids = [s.strip() for s in args.ids.split(",") if s.strip() and not s.strip().isdigit()]
        if bad_ids:
            p.error(f"--ids must be a comma-separated list of integers; bad values: {', '.join(bad_ids)}")

    if args.since:
        try:
            datetime.strptime(args.since, "%Y-%m-%d")
        except ValueError:
            p.error(f"--since must be YYYY-MM-DD, got {args.since!r}")
    if args.until:
        try:
            datetime.strptime(args.until, "%Y-%m-%d")
        except ValueError:
            p.error(f"--until must be YYYY-MM-DD, got {args.until!r}")
    if args.since and args.until and args.since > args.until:
        p.error(f"--since ({args.since}) must be <= --until ({args.until})")
    if args.concurrency < 1 or args.concurrency > 32:
        p.error("--concurrency must be between 1 and 32")
    if args.per_page < 1 or args.per_page > 500:
        p.error("--per-page must be between 1 and 500 (Travio caps at 500)")

    return args


# ─── Discovery: page-walk backwards, collect IDs in range ──────────────────

def discover_ids_by_date(
    travio: TravioClient,
    since: str,
    until: str,
    per_page: int,
    log: Logger,
    max_ids: int = 0,
) -> list[int]:
    """Page-walk Travio /rest/reservations backwards, return IDs whose `date`
    is in [since, until] (inclusive). Default order is ascending by id, so the
    newest IDs are at the END — we start at the last page and walk backwards.

    Early-exit strategy: each page exposes a min/max booking date. If the
    entire page is newer than `until`, keep walking backward. If the entire
    page is older than `since`, we've crossed the boundary and can stop. Note
    that `date` is the booking creation date which is roughly monotonic with
    id for active reservations, but in principle could diverge for very old
    reservations that were updated. We also stop after 5 consecutive pages
    whose dates are within/near the range but still contain no hits as a
    safety net.
    """
    # First, learn the total page count from a per_page=1 peek
    log.info("discover_peeking_total", per_page=1)
    first_page = travio.list_reservations_page(1, per_page=1)
    tot = first_page.get("tot", 0)
    if tot == 0:
        log.info("discover_empty", note="Travio reports 0 reservations")
        return []
    total_pages = (tot + per_page - 1) // per_page
    log.info("discover_starting", tot=tot, total_pages=total_pages,
             since=since, until=until, per_page=per_page)

    found_ids: list[int] = []
    consecutive_empty = 0
    page = total_pages
    pages_scanned = 0

    while page > 0:
        try:
            resp = travio.list_reservations_page(page, per_page=per_page)
        except TravioError as e:
            log.error("discover_page_failed", page=page, error=str(e))
            raise

        items = resp.get("list", [])
        pages_scanned += 1
        if not items:
            break

        # Per-date in-range check. Page items are ascending by id and usually
        # also roughly ascending by booking date, but use min/max date to avoid
        # relying on item ordering inside the page.
        page_dates = [str(r.get("date", ""))[:10] for r in items if r.get("date")]
        page_min_date = min(page_dates) if page_dates else ""
        page_max_date = max(page_dates) if page_dates else ""
        in_range = [r for r in items if _date_in_range(r.get("date"), since, until)]

        if in_range:
            found_ids.extend(r["id"] for r in in_range)
            consecutive_empty = 0
        else:
            # If the entire page is newer than the requested window, keep
            # walking backwards; these are not "empty" in the stop-threshold
            # sense, they are just above the range.
            if page_min_date and page_min_date > until:
                log.info("discover_page_newer_than_range",
                         page=page, page_min_date=page_min_date,
                         page_max_date=page_max_date, until=until)
            # If the entire page is older than the requested window, we have
            # crossed the boundary. The remaining pages will be older still.
            elif page_max_date and page_max_date < since:
                log.info("discover_crossed_boundary",
                         page=page, page_max_date=page_max_date, since=since,
                         found_so_far=len(found_ids))
                break
            else:
                consecutive_empty += 1

        if max_ids and len(found_ids) >= max_ids:
            log.info("discover_max_ids_reached", max_ids=max_ids)
            found_ids = found_ids[:max_ids]
            break

        # Safety net: too many empty pages in a row, stop
        if consecutive_empty >= 5:
            log.info("discover_too_many_empty_pages", consecutive_empty=consecutive_empty)
            break

        if pages_scanned % 10 == 0:
            log.info("discover_progress", page=page, found_so_far=len(found_ids),
                     pages_scanned=pages_scanned)
        page -= 1

    log.info("discover_done", pages_scanned=pages_scanned, found=len(found_ids))
    return found_ids


def _date_in_range(d: str | None, since: str, until: str) -> bool:
    if not d:
        return False
    # Travio `date` is YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
    s = d[:10]
    return since <= s <= until


# ─── Per-id refresh + upsert ──────────────────────────────────────────────

def refresh_one(travio: TravioClient, writer: NeonWriter, travio_id: int,
                dry_run: bool, log: Logger) -> dict:
    """Fetch one reservation and upsert. Returns the upsert outcome dict."""
    try:
        r = travio.get_reservation(travio_id)
    except TravioError as e:
        return {"travio_id": travio_id, "ok": False, "error": f"travio: {e}"}

    if r is None:
        return {"travio_id": travio_id, "ok": False, "error": "not_found"}

    if dry_run:
        return {"travio_id": travio_id, "ok": True, "action": "dry_run_skip"}

    try:
        outcome = writer.upsert_reservation(r)
    except Exception as e:
        return {"travio_id": travio_id, "ok": False,
                "error": f"neon: {type(e).__name__}: {e}"}

    return {
        "travio_id": travio_id,
        "ok": True,
        "action": "created" if outcome["created"]
                  else "updated" if outcome["updated"]
                  else "unchanged",
    }


def run_with_concurrency(travio: TravioClient, writer: NeonWriter, ids: list[int],
                          concurrency: int, dry_run: bool, log: Logger) -> dict:
    """Refresh all IDs in parallel. Returns aggregate stats + per-id errors.

    In dry-run mode, we skip the per-id re-fetch entirely (the user just wants
    to see what would be touched, not exercise the whole pipeline).
    """
    created = updated = unchanged = failed = 0
    errors: list[dict] = []
    started = time.monotonic()

    if dry_run:
        log.info("dry_run_skipping_per_id_fetch", ids_count=len(ids))
        return {
            "found": len(ids),
            "created": 0,
            "updated": 0,
            "unchanged": 0,
            "failed": 0,
            "duration_seconds": 0.0,
            "errors": [],
        }

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {
            ex.submit(refresh_one, travio, writer, tid, dry_run, log): tid
            for tid in ids
        }
        done_count = 0
        for fut in as_completed(futures):
            tid = futures[fut]
            done_count += 1
            result = fut.result()
            if not result.get("ok"):
                failed += 1
                errors.append(result)
                log.warn("refresh_failed", **result)
            elif result.get("action") == "created":
                created += 1
            elif result.get("action") == "updated":
                updated += 1
            else:
                unchanged += 1
            if done_count % 50 == 0 or done_count == len(ids):
                log.info("refresh_progress", done=done_count, total=len(ids),
                         created=created, updated=updated, unchanged=unchanged, failed=failed)

    duration = time.monotonic() - started
    return {
        "found": len(ids),
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "failed": failed,
        "duration_seconds": round(duration, 2),
        "errors": errors[:20],  # cap for readability
    }


# ─── main ─────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_args()

    # Load .env from cwd if present (mirrors tests/smoke.py)
    from pathlib import Path as _P
    env_path = _P.cwd() / ".env"
    if env_path.exists():
        import os
        for raw in env_path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

    cfg = Config.load()
    log = Logger("backfill", cfg.log_level)

    travio = TravioClient(
        base_url=cfg.travio_base_url,
        client_id=cfg.travio_id,
        client_key=cfg.travio_key,
        requests_per_minute=cfg.travio_requests_per_minute,
    )
    writer = NeonWriter(cfg.neon_database_url)

    started = time.monotonic()
    try:
        if args.ids:
            ids = [int(s.strip()) for s in args.ids.split(",") if s.strip()]
            log.info("explicit_ids_mode", count=len(ids))
        else:
            ids = discover_ids_by_date(
                travio, args.since, args.until,
                per_page=args.per_page, log=log, max_ids=args.max_ids,
            )
    except TravioError as e:
        log.error("discovery_failed", error=str(e))
        return 3

    if not ids:
        log.info("nothing_to_do", note="no IDs matched the criteria")
        if args.json:
            print(json.dumps({
                "found": 0, "created": 0, "updated": 0,
                "unchanged": 0, "failed": 0, "duration_seconds": 0.0,
                "errors": [],
            }))
        return 0

    result = run_with_concurrency(
        travio, writer, ids, args.concurrency, args.dry_run, log,
    )
    result["discovery_seconds"] = round(time.monotonic() - started, 2)
    result["dry_run"] = args.dry_run

    if args.json:
        print(json.dumps(result, default=str))
    else:
        log.info("backfill_complete", **result)

    return 0 if result["failed"] == 0 else 5


if __name__ == "__main__":
    sys.exit(main())
