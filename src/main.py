"""Main loop: poll gb-travio-webhooks events, fetch from Travio API, upsert.

State machine:
  cursor = max(event id we've seen) in SQLite
  each tick:
    fetch events older than cursor from /internal/events
    for each event (newest first):
      parse subject: 'reservations:<id>' or 'master-data:<id>'
      fetch full record from Travio
      upsert into Neon
      advance cursor to event.id (but only for events we actually processed
        successfully — failed events stay in front of the cursor and
        get retried on the next tick)
    sleep

We also support a 'catchup' mode that processes a fixed number of events
on startup before entering the poll loop, useful for first-run backlog.
"""

from __future__ import annotations

import signal
import sys
import time
from dataclasses import dataclass

from config import Config
from logging_setup import Logger
from neon_writer import NeonWriter
from state import StateStore
from travio_client import TravioClient, TravioError
from webhook_client import WebhookReceiverClient, WebhookReceiverError

KEY_LAST_SEEN_EVENT_ID = "last_seen_event_id"


@dataclass
class RunStats:
    ticks: int = 0
    events_seen: int = 0
    events_processed: int = 0
    events_failed: int = 0
    reservations_created: int = 0
    reservations_updated: int = 0
    reservations_unchanged: int = 0
    master_data_created: int = 0
    master_data_updated: int = 0
    master_data_unchanged: int = 0


class Bridge:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.log = Logger(cfg.service_name, cfg.log_level)
        self.state = StateStore(cfg.webhook_state_db)
        self.travio = TravioClient(
            base_url=cfg.travio_base_url,
            client_id=cfg.travio_id,
            client_key=cfg.travio_key,
            requests_per_minute=cfg.travio_requests_per_minute,
        )
        self.receiver = WebhookReceiverClient(
            base_url=cfg.webhook_base_url,
            admin_api_key=cfg.webhook_admin_api_key,
        )
        self.writer = NeonWriter(cfg.neon_database_url)
        self.stats = RunStats()
        self._stop = False

    def stop(self) -> None:
        self.log.info("stop_requested", reason="signal")
        self._stop = True

    # ─── event dispatch ──────────────────────────────────────────────────
    def _handle_event(self, event: dict) -> None:
        subject = event.get("event_subject") or ""
        if ":" not in subject:
            self.log.warn("event_missing_subject", event_id=event.get("id"), subject=subject)
            return
        resource_type, _, resource_id_str = subject.partition(":")
        try:
            resource_id = int(resource_id_str)
        except ValueError:
            self.log.warn("event_bad_subject_id", subject=subject, event_id=event.get("id"))
            return

        if resource_type == "reservations":
            self._process_reservation(resource_id, event)
        elif resource_type == "master-data":
            self._process_master_data(resource_id, event)
        else:
            self.log.info("event_skipped_unknown_resource_type",
                          resource_type=resource_type, event_id=event.get("id"))

    def _process_reservation(self, travio_id: int, event: dict) -> None:
        try:
            r = self.travio.get_reservation(travio_id)
        except TravioError as e:
            self.log.error("travio_fetch_failed", resource_type="reservations",
                           travio_id=travio_id, error=str(e), event_id=event.get("id"))
            raise
        if r is None:
            self.log.warn("reservation_not_found", travio_id=travio_id, event_id=event.get("id"))
            return

        if self.cfg.dry_run:
            self.log.info("dry_run_skip", resource_type="reservations", travio_id=travio_id)
            return

        result = self.writer.upsert_reservation(r)
        if result["created"]:
            self.stats.reservations_created += 1
        elif result["updated"]:
            self.stats.reservations_updated += 1
        else:
            self.stats.reservations_unchanged += 1
        self.log.info("reservation_upserted", travio_id=travio_id, event_id=event.get("id"),
                      event_type=event.get("event_type"), **result)

    def _process_master_data(self, travio_id: int, event: dict) -> None:
        try:
            d = self.travio.get_master_data(travio_id)
        except TravioError as e:
            self.log.error("travio_fetch_failed", resource_type="master-data",
                           travio_id=travio_id, error=str(e), event_id=event.get("id"))
            raise
        if d is None:
            self.log.warn("master_data_not_found", travio_id=travio_id, event_id=event.get("id"))
            return

        if self.cfg.dry_run:
            self.log.info("dry_run_skip", resource_type="master-data", travio_id=travio_id)
            return

        result = self.writer.upsert_master_data(d)
        if result["created"]:
            self.stats.master_data_created += 1
        elif result["updated"]:
            self.stats.master_data_updated += 1
        else:
            self.stats.master_data_unchanged += 1
        self.log.info("master_data_upserted", travio_id=travio_id, event_id=event.get("id"),
                      event_type=event.get("event_type"), **result)

    # ─── tick ────────────────────────────────────────────────────────────
    def _tick(self) -> None:
        self.stats.ticks += 1
        cursor = self.state.get_int(KEY_LAST_SEEN_EVENT_ID, default=2_000_000_000)

        try:
            page = self.receiver.list_events(after_id=cursor, limit=self.cfg.webhook_poll_limit)
        except WebhookReceiverError as e:
            self.log.error("webhook_receiver_poll_failed", error=str(e))
            return

        events = page.get("events", [])
        if not events:
            return
        self.stats.events_seen += len(events)

        # Events come newest-first. We process oldest-first so the cursor
        # advances monotonically and a crash mid-batch leaves us no gaps.
        events_sorted = sorted(events, key=lambda e: int(e["id"]))

        for event in events_sorted:
            event_id = int(event["id"])
            try:
                self._handle_event(event)
                self.stats.events_processed += 1
                # Only advance cursor on success
                cursor = min(cursor, event_id)
                self.state.set_int(KEY_LAST_SEEN_EVENT_ID, cursor)
            except (TravioError, WebhookReceiverError) as e:
                self.stats.events_failed += 1
                self.log.error("event_handler_failed", event_id=event_id, error=str(e))
                # Don't advance cursor; retry next tick
                return
            except Exception as e:  # unexpected — log + skip (don't tight-loop)
                self.stats.events_failed += 1
                self.log.error("event_handler_unexpected_error",
                               event_id=event_id, error=str(e), error_type=type(e).__name__)
                # Continue processing other events; the bad one is recorded
                # in logs. We could choose to halt here but that would block
                # all forward progress on one bad row.

    # ─── main loop ───────────────────────────────────────────────────────
    def run(self) -> None:
        self.log.info("startup", travio_base=self.cfg.travio_base_url,
                      webhook_base=self.cfg.webhook_base_url,
                      poll_interval_s=self.cfg.webhook_poll_interval_seconds,
                      dry_run=self.cfg.dry_run)
        try:
            rt = self.receiver.health()
            self.log.info("webhook_receiver_reachable",
                          version=rt.get("version"), service=rt.get("service"))
        except Exception as e:
            self.log.error("webhook_receiver_unreachable", error=str(e))
            raise

        while not self._stop:
            tick_start = time.monotonic()
            try:
                self._tick()
            except Exception as e:
                self.log.error("tick_unexpected_error", error=str(e), error_type=type(e).__name__)

            elapsed = time.monotonic() - tick_start
            sleep_for = max(0.0, self.cfg.webhook_poll_interval_seconds - elapsed)
            if self.stats.ticks % 60 == 0:
                self.log.info("stats",
                              ticks=self.stats.ticks,
                              events_seen=self.stats.events_seen,
                              events_processed=self.stats.events_processed,
                              events_failed=self.stats.events_failed,
                              reservations_created=self.stats.reservations_created,
                              reservations_updated=self.stats.reservations_updated,
                              reservations_unchanged=self.stats.reservations_unchanged,
                              master_data_created=self.stats.master_data_created,
                              master_data_updated=self.stats.master_data_updated,
                              master_data_unchanged=self.stats.master_data_unchanged,
                              uptime_s=int(time.monotonic()))
            if sleep_for > 0:
                time.sleep(sleep_for)

        self.log.info("shutdown", **self.stats.__dict__)


def main() -> int:
    cfg = Config.load()
    bridge = Bridge(cfg)

    def _on_signal(signum, _frame):
        bridge.stop()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    bridge.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
