"""End-to-end smoke test: configure with real env, run one tick, report.

Usage:
    python3 -m tests.smoke
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Add the src/ directory to the import path FIRST
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Load .env from the project root if present
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if ENV_PATH.exists():
    for raw in ENV_PATH.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from config import Config  # noqa: E402
from logging_setup import Logger  # noqa: E402
from travio_client import TravioClient  # noqa: E402
from webhook_client import WebhookReceiverClient  # noqa: E402
from neon_writer import NeonWriter  # noqa: E402


def main() -> int:
    log = Logger("smoke-test", "info")
    try:
        cfg = Config.load()
    except SystemExit as e:
        log.error("config_load_failed", code=e.code)
        return 1

    log.info("smoke_test_start",
             travio_base=cfg.travio_base_url,
             webhook_base=cfg.webhook_base_url,
             dry_run=cfg.dry_run)

    # 1. Webhook receiver reachable
    try:
        rt = WebhookReceiverClient(cfg.webhook_base_url, cfg.webhook_admin_api_key).health()
        log.info("webhook_receiver_ok", version=rt.get("version"))
    except Exception as e:
        log.error("webhook_receiver_failed", error=str(e))
        return 2

    # 2. Travio auth + reservation fetch
    travio = TravioClient(
        base_url=cfg.travio_base_url,
        client_id=cfg.travio_id,
        client_key=cfg.travio_key,
        requests_per_minute=cfg.travio_requests_per_minute,
    )
    try:
        r = travio.get_reservation(1107373)
        if r is None:
            log.error("travio_reservation_404", travio_id=1107373)
            return 3
        log.info("travio_reservation_ok",
                 travio_id=r.get("id"),
                 num=r.get("num"),
                 status=r.get("status"),
                 pax_count=len(r.get("pax") or []),
                 services_count=len(r.get("services") or []))
    except Exception as e:
        log.error("travio_fetch_failed", error=str(e))
        return 4

    # 3. Neon connectivity + write (unless dry-run)
    if cfg.dry_run:
        log.info("smoke_dry_run_skipping_neon_write")
    else:
        try:
            writer = NeonWriter(cfg.neon_database_url)
            result = writer.upsert_reservation(r)
            log.info("neon_reservation_upsert_ok", travio_id=r.get("id"), **result)
        except Exception as e:
            log.error("neon_upsert_failed", error=str(e), error_type=type(e).__name__)
            return 5

    log.info("smoke_test_pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
