"""On-demand single-record refresh.

Fetches one reservation or master_data record from Travio's REST API and
upserts it to gb-udb. Use this when:
- you spot a stale row in Neon (e.g. status is wrong, child data missing)
- the realtime bridge is down and you need to force a refresh
- someone reports "the dashboard shows X but Travio shows Y"

Usage:
    python3 -m src.refresh_one reservation 1107373
    python3 -m src.refresh_one master_data 412156
    python3 -m src.refresh_one reservation 1107373 --dry-run --json
    python3 -m src.refresh_one master_data 999999  # exits 4 if not found

Exit codes:
    0  success
    1  config / connection error
    2  bad CLI args
    3  Travio fetch failed
    4  record not found in Travio
    5  Neon write failed
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from config import Config
from logging_setup import Logger
from travio_client import TravioClient, TravioError
from neon_writer import NeonWriter


RESOURCE_TYPES = {
    "reservation": ("reservations", "_process_reservation_via_writer"),
    "master_data": ("master-data", "_process_master_data_via_writer"),
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch one Travio record by ID and upsert to gb-udb",
    )
    parser.add_argument("resource_type", choices=sorted(RESOURCE_TYPES.keys()),
                        help="Type of record to refresh")
    parser.add_argument("travio_id", type=int,
                        help="Travio numeric ID of the record")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch from Travio but do not write to Neon")
    parser.add_argument("--json", action="store_true",
                        help="Emit a single JSON object as the only output line")
    args = parser.parse_args()

    # Allow project-root cwd: load .env if present (mirrors tests/smoke.py)
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
    log = Logger("refresh-one", cfg.log_level)

    result: dict = {
        "resource_type": args.resource_type,
        "travio_id": args.travio_id,
        "dry_run": args.dry_run,
    }

    travio = TravioClient(
        base_url=cfg.travio_base_url,
        client_id=cfg.travio_id,
        client_key=cfg.travio_key,
        requests_per_minute=cfg.travio_requests_per_minute,
    )

    # 1. Fetch from Travio
    try:
        if args.resource_type == "reservation":
            record = travio.get_reservation(args.travio_id)
        else:
            record = travio.get_master_data(args.travio_id)
    except TravioError as e:
        result["error"] = "travio_fetch_failed"
        result["detail"] = str(e)
        if args.json:
            print(json.dumps(result))
        else:
            log.error("travio_fetch_failed", **result)
        return 3

    if record is None:
        result["error"] = "not_found"
        if args.json:
            print(json.dumps(result))
        else:
            log.warn("travio_record_not_found", **result)
        return 4

    # Add some context
    if args.resource_type == "reservation":
        result["reservation_number"] = record.get("num")
        result["status"] = record.get("status")
        result["customer_name"] = (record.get("pax") or [{}])[0].get("name") if record.get("pax") else None
    else:
        result["full_name"] = record.get("full_name")
        result["profile_type"] = record.get("profile_type")

    # 2. Write to Neon (unless dry-run)
    if args.dry_run:
        result["action"] = "dry_run_skip"
    else:
        try:
            writer = NeonWriter(cfg.neon_database_url)
            if args.resource_type == "reservation":
                outcome = writer.upsert_reservation(record)
            else:
                outcome = writer.upsert_master_data(record)
            result["upsert"] = outcome
            if outcome["created"]:
                result["action"] = "created"
            elif outcome["updated"]:
                result["action"] = "updated"
            else:
                result["action"] = "unchanged"
        except Exception as e:
            result["error"] = "neon_write_failed"
            result["detail"] = f"{type(e).__name__}: {e}"
            if args.json:
                print(json.dumps(result))
            else:
                log.error("neon_write_failed", **result)
            return 5

    # 3. Emit result
    if args.json:
        print(json.dumps(result, default=str))
    else:
        # Pretty human-readable summary
        if args.dry_run:
            log.info("dry_run_complete", **result)
        else:
            log.info("refresh_complete", **result)
        if args.resource_type == "reservation":
            print(f"  reservation_number: {result.get('reservation_number')}")
            print(f"  status:             {result.get('status')}")
            print(f"  customer_name:      {result.get('customer_name')}")
        else:
            print(f"  full_name:    {result.get('full_name')}")
            print(f"  profile_type: {result.get('profile_type')}")
        if "upsert" in result:
            print(f"  upsert:       {result['upsert']}")
        print(f"  action:       {result.get('action')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
