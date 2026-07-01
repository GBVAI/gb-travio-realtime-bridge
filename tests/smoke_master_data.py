"""Smoke test for the master_data writer path."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import Config  # noqa: E402
from logging_setup import Logger  # noqa: E402
from travio_client import TravioClient  # noqa: E402
from neon_writer import NeonWriter  # noqa: E402


def main() -> int:
    log = Logger("smoke-master-data", "info")
    cfg = Config.load()

    travio = TravioClient(
        base_url=cfg.travio_base_url,
        client_id=cfg.travio_id,
        client_key=cfg.travio_key,
        requests_per_minute=cfg.travio_requests_per_minute,
    )

    log.info("fetching_test_master_data", travio_id=412156)
    d = travio.get_master_data(412156)
    if d is None:
        log.error("master_data_not_found", travio_id=412156)
        return 1
    log.info("got_master_data", travio_id=d.get("id"),
             full_name=d.get("full_name"),
             contact_count=len(d.get("contacts") or []),
             address_count=len(d.get("addresses") or []))

    if cfg.dry_run:
        log.info("dry_run_skipping_write")
        return 0

    writer = NeonWriter(cfg.neon_database_url)
    result = writer.upsert_master_data(d)
    log.info("master_data_upserted", travio_id=d.get("id"), **result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
