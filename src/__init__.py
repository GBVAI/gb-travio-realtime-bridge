"""GB Travio Realtime Bridge.

Polls the gb-travio-webhooks internal event API on Railway and, for each new
event, fetches the full record from the Travio REST API directly from this host
(this host's egress IP is on Travio's allowlist; Railway's is not, which is why
gb-automations on Railway cannot reach Travio and the real-time lane has been
dead since ~June 5, 2026).

Then upserts the record into gb-udb (Neon Postgres), with the same derived
fields, sub-tables, and snapshots that gb-automations was doing before the
IP-allowlist incident.

The nightly full-mirror (gb-travioetl/neon_bulk_load.py) remains the safety
net. This service makes intraday updates current; the freshness guard in
neon_bulk_load.py (commit 64cc865) prevents the mirror from clobbering
intraday writes.
"""

__version__ = "0.1.0"
