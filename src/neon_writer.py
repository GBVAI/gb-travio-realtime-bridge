"""Neon (gb-udb) writer. Ports the reservation + master_data upserts from
gbbookingincentives/automations/src/lib/neon-writer.ts and
.../master-data-writer.ts to Python + psycopg2.

The shapes are kept byte-for-byte compatible with the TS versions so a
diff between an upsert here and the equivalent TS one is a useful review
artifact. We do not, however, replicate the contact_points logic — that
is gb-automations-only territory and not used by gb-travioetl either,
so we omit it for v1.0 to keep scope tight. The fields that gb-travioetl
uses (master_data, master_data_contacts, master_data_addresses) ARE
populated.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.extras

from travio_client import TravioClient, TRAVIO_STATUS_NAMES


def _sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@contextmanager
def _tx(conn: psycopg2.extensions.connection) -> Iterator[psycopg2.extensions.cursor]:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        yield cur
        cur.execute("COMMIT")
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        cur.close()


class NeonWriter:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    @contextmanager
    def _conn(self) -> Iterator[psycopg2.extensions.connection]:
        conn = psycopg2.connect(self.dsn)
        try:
            yield conn
        finally:
            conn.close()

    # ─── Reservation ─────────────────────────────────────────────────────
    def upsert_reservation(self, r: dict) -> dict:
        """Return {'created': bool, 'updated': bool, 'unchanged': bool}."""
        source_payload = json.dumps(r, default=str)
        payload_sha = _sha256(source_payload)

        status_id, status_name = TravioClient.status_info(r.get("status"))

        # Customer name/email
        customer_name, customer_email = self._extract_customer(r)

        # Destination
        description = (r.get("description") or "").strip() or None
        destination = description.split("\n")[0].strip() if description else None

        # Cost / price
        cost = r.get("cost") or {}
        price = r.get("price") or {}
        cost_gross = cost.get("gross")
        cost_commission = cost.get("commission")
        cost_net = cost.get("net")
        price_gross = price.get("gross")
        price_commission = price.get("commission")
        price_net = price.get("net")
        commission = price.get("commission")

        # Operator cost = SUM(services.rows[].cost.net)
        operator_cost = 0.0
        for service in r.get("services") or []:
            for row in service.get("rows") or []:
                rc = (row.get("cost") or {}).get("net")
                if rc is not None:
                    operator_cost += float(rc)

        is_agency = (commission or 0) > 0
        margin = 0.0
        if price_gross is not None and operator_cost > 0:
            if is_agency and price_net is not None:
                margin = float(price_net) - operator_cost
            else:
                margin = float(price_gross) - operator_cost
        margin_pct = (margin / float(price_gross) * 100) if (price_gross and price_gross > 0) else None

        # Payment status
        amount_paid = 0.0
        amount_remaining = price_gross if price_gross else None
        has_accounting = False
        for entry in r.get("accounting_entries") or []:
            if entry.get("type") == "client":
                payments = entry.get("payments") or {}
                if "paid" in payments:
                    amount_paid = float(payments.get("paid") or 0)
                if "remaining" in payments and payments["remaining"] is not None:
                    amount_remaining = float(payments["remaining"])
                    has_accounting = True
        is_fully_paid = has_accounting and (amount_remaining is not None and amount_remaining <= 0)

        has_insurance, insurance_margin = TravioClient.has_insurance(r)

        is_international = TravioClient.is_international(r)

        pax_count = len(r.get("pax") or [])

        last_status_date = self._latest_status_date(r.get("status_history"))

        booking_source = (
            (r.get("_meta") or {}).get("creation_source")
            or ("api" if (r.get("_meta") or {}).get("creation_api_key") else "internal")
        )
        is_web_booking = (
            booking_source == "api"
            or (r.get("_meta") or {}).get("creation_api_key") is not None
        )

        agent_travio_id = TravioClient.link_id(r.get("user"))

        with self._conn() as conn:
            with _tx(conn) as cur:
                # 1. Agent upsert (only if user is unfolded with name info)
                user_obj = r.get("user")
                if isinstance(user_obj, dict):
                    user_id = TravioClient.link_id(user_obj)
                    if user_id:
                        cur.execute(
                            """
                            INSERT INTO agents (travio_id, name, surname, username, enabled, updated_at)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (travio_id) DO UPDATE SET
                              name     = COALESCE(EXCLUDED.name, agents.name),
                              surname  = COALESCE(EXCLUDED.surname, agents.surname),
                              username = COALESCE(EXCLUDED.username, agents.username),
                              enabled  = EXCLUDED.enabled,
                              updated_at = NOW()
                            """,
                            (
                                user_id,
                                user_obj.get("name"),
                                user_obj.get("surname"),
                                user_obj.get("username"),
                                True,
                            ),
                        )

                # 2. Check existence (for created/updated tracking)
                cur.execute(
                    "SELECT payload_sha256 FROM reservations WHERE travio_id = %s",
                    (r["id"],),
                )
                row = cur.fetchone()
                existed = row is not None
                previous_sha = row[0] if row else None

                # 3. Main reservation UPSERT
                cur.execute(
                    """
                    INSERT INTO reservations (
                        travio_id, reservation_number, reservation_year, booking_date,
                        departure_date, return_date, status, status_name,
                        confirmation_date, cancellation_date, description, reference,
                        requested_by, first_pax, due_date, last_status_date,
                        heading_master_data_id, client_master_data_id,
                        invoice_client_master_data_id, payment_client_master_data_id,
                        promoter_master_data_id, network_master_data_id, user_master_data_id,
                        customer_name, customer_email, destination,
                        cost_gross, cost_commission, cost_net,
                        price_gross, price_commission, price_net,
                        operator_cost, commission, margin, margin_percentage,
                        amount_paid, amount_remaining, is_fully_paid,
                        is_international, has_insurance, insurance_margin,
                        booking_source, is_web_booking, pax_count,
                        source_payload, payload_sha256, canonical_header_hash,
                        travio_last_update, agent_travio_id, synced_at, updated_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        NOW(), NOW()
                    )
                    ON CONFLICT (travio_id) DO UPDATE SET
                        reservation_number           = EXCLUDED.reservation_number,
                        reservation_year             = EXCLUDED.reservation_year,
                        booking_date                 = EXCLUDED.booking_date,
                        departure_date               = EXCLUDED.departure_date,
                        return_date                  = EXCLUDED.return_date,
                        status                       = EXCLUDED.status,
                        status_name                  = EXCLUDED.status_name,
                        confirmation_date            = EXCLUDED.confirmation_date,
                        cancellation_date            = EXCLUDED.cancellation_date,
                        description                  = EXCLUDED.description,
                        reference                    = EXCLUDED.reference,
                        requested_by                 = EXCLUDED.requested_by,
                        first_pax                    = EXCLUDED.first_pax,
                        due_date                     = EXCLUDED.due_date,
                        last_status_date             = EXCLUDED.last_status_date,
                        heading_master_data_id       = EXCLUDED.heading_master_data_id,
                        client_master_data_id        = EXCLUDED.client_master_data_id,
                        invoice_client_master_data_id= EXCLUDED.invoice_client_master_data_id,
                        payment_client_master_data_id= EXCLUDED.payment_client_master_data_id,
                        promoter_master_data_id      = EXCLUDED.promoter_master_data_id,
                        network_master_data_id       = EXCLUDED.network_master_data_id,
                        user_master_data_id          = EXCLUDED.user_master_data_id,
                        customer_name                = EXCLUDED.customer_name,
                        customer_email               = EXCLUDED.customer_email,
                        destination                  = EXCLUDED.destination,
                        cost_gross                   = EXCLUDED.cost_gross,
                        cost_commission              = EXCLUDED.cost_commission,
                        cost_net                     = EXCLUDED.cost_net,
                        price_gross                  = EXCLUDED.price_gross,
                        price_commission             = EXCLUDED.price_commission,
                        price_net                    = EXCLUDED.price_net,
                        operator_cost                = EXCLUDED.operator_cost,
                        commission                   = EXCLUDED.commission,
                        margin                       = EXCLUDED.margin,
                        margin_percentage            = EXCLUDED.margin_percentage,
                        amount_paid                  = EXCLUDED.amount_paid,
                        amount_remaining             = EXCLUDED.amount_remaining,
                        is_fully_paid                = EXCLUDED.is_fully_paid,
                        is_international             = EXCLUDED.is_international,
                        has_insurance                = EXCLUDED.has_insurance,
                        insurance_margin             = EXCLUDED.insurance_margin,
                        booking_source               = EXCLUDED.booking_source,
                        is_web_booking               = EXCLUDED.is_web_booking,
                        pax_count                    = EXCLUDED.pax_count,
                        source_payload               = EXCLUDED.source_payload,
                        payload_sha256               = EXCLUDED.payload_sha256,
                        travio_last_update           = EXCLUDED.travio_last_update,
                        agent_travio_id              = EXCLUDED.agent_travio_id,
                        synced_at                    = NOW(),
                        updated_at                   = NOW()
                    """,
                    (
                        r["id"],
                        r.get("num"),
                        r.get("year"),
                        r.get("date"),
                        r.get("from"),
                        r.get("to"),
                        status_id,
                        status_name,
                        r.get("confirmation_date"),
                        r.get("cancellation_date"),
                        description,
                        r.get("reference"),
                        r.get("requested_by"),
                        r.get("first_pax"),
                        r.get("due"),
                        last_status_date,
                        TravioClient.link_id(r.get("heading")),
                        TravioClient.link_id(r.get("client")),
                        TravioClient.link_id(r.get("invoice_client")),
                        TravioClient.link_id(r.get("payment_client")),
                        TravioClient.link_id(r.get("promoter")),
                        TravioClient.link_id(r.get("network")),
                        TravioClient.link_id(r.get("user")),
                        customer_name,
                        customer_email,
                        destination,
                        cost_gross, cost_commission, cost_net,
                        price_gross, price_commission, price_net,
                        operator_cost if operator_cost > 0 else None,
                        commission, margin, margin_pct,
                        amount_paid, amount_remaining, is_fully_paid,
                        is_international, has_insurance, insurance_margin,
                        booking_source, is_web_booking, pax_count,
                        source_payload, payload_sha, None,
                        (r.get("_meta") or {}).get("last_update"),
                        agent_travio_id,
                    ),
                )

                # 4. Status history
                for entry in r.get("status_history") or []:
                    sid, sname = TravioClient.status_info(entry.get("status"))
                    user_obj = entry.get("user")
                    user_id = TravioClient.link_id(user_obj) if user_obj else None
                    user_name = None
                    if isinstance(user_obj, dict):
                        n = f"{user_obj.get('name') or ''} {user_obj.get('surname') or ''}".strip()
                        user_name = n or None
                    cur.execute(
                        """
                        INSERT INTO reservation_status_history (
                            reservation_travio_id, status, status_name, changed_at,
                            changed_by_travio_id, changed_by_name, gross_price, amount_paid
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (reservation_travio_id, status, changed_at) DO NOTHING
                        """,
                        (r["id"], sid, sname, entry.get("date"),
                         user_id, user_name, price_gross, amount_paid),
                    )

                # 5. Services (delete + insert)
                cur.execute("DELETE FROM reservation_services WHERE reservation_travio_id = %s", (r["id"],))
                position = 0
                for service in r.get("services") or []:
                    for row in service.get("rows") or []:
                        frm = (row.get("from") or {}).get("date") if isinstance(row.get("from"), dict) else None
                        to = (row.get("to") or {}).get("date") if isinstance(row.get("to"), dict) else None
                        cur.execute(
                            """
                            INSERT INTO reservation_services (
                                reservation_travio_id, position, source_payload,
                                service_name, service_type,
                                service_travio_id, scope, from_date, to_date, quantity,
                                status, on_site, price_gross, price_net,
                                cost_gross, cost_net, price_commission, cost_commission,
                                supplier_master_data_id, hotel_reference_id
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            (
                                r["id"], position, json.dumps(row, default=str),
                                service.get("name"), service.get("type"),
                                row.get("id"), row.get("scope"),
                                frm, to, row.get("quantity"),
                                row.get("status"), row.get("on_site"),
                                (row.get("price") or {}).get("gross"),
                                (row.get("price") or {}).get("net"),
                                (row.get("cost") or {}).get("gross"),
                                (row.get("cost") or {}).get("net"),
                                row.get("price_commission"),
                                row.get("cost_commission"),
                                row.get("supplier_master_data_id"),
                                row.get("hotel_reference_id"),
                            ),
                        )
                        position += 1

                # 6. Pax (delete + insert)
                cur.execute("DELETE FROM reservation_pax WHERE reservation_travio_id = %s", (r["id"],))
                for p in r.get("pax") or []:
                    cur.execute(
                        """
                        INSERT INTO reservation_pax (
                            reservation_travio_id, source_payload,
                            name, surname, email,
                            travio_pax_id, phone, gender, birth_date, birth_place,
                            nationality, tax_code, age, language, notes,
                            doc_type, doc_number, doc_issued, doc_expiry,
                            address, city, province, postal_code, country_id, room_index
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            r["id"], json.dumps(p, default=str),
                            p.get("name"), p.get("surname"), p.get("email"),
                            p.get("id"), p.get("phone"), p.get("gender"),
                            p.get("birth_date"), p.get("birth_place"),
                            p.get("nationality"), p.get("tax_code"),
                            p.get("age"), p.get("language"), p.get("notes"),
                            p.get("doc_type"), p.get("doc_number"),
                            p.get("doc_issued"), p.get("doc_expiry"),
                            p.get("address"), p.get("city"),
                            p.get("province"), p.get("postal_code"),
                            p.get("country_id"), p.get("room_index"),
                        ),
                    )

                # 7. Snapshot if changed
                if payload_sha != previous_sha:
                    cur.execute(
                        """
                        INSERT INTO reservation_snapshots (
                            travio_id, payload_sha256, source_payload, captured_at
                        ) VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (travio_id, payload_sha256) DO NOTHING
                        """,
                        (r["id"], payload_sha, source_payload),
                    )

                # 8. Enqueue CRM sync
                cur.execute(
                    """
                    INSERT INTO crm_sync_state (reservation_travio_id, crm_target)
                    VALUES (%s, 'twenty')
                    ON CONFLICT (reservation_travio_id, crm_target) DO NOTHING
                    """,
                    (r["id"],),
                )

                return {
                    "created": not existed,
                    "updated": existed and payload_sha != previous_sha,
                    "unchanged": existed and payload_sha == previous_sha,
                }

    # ─── Master Data ─────────────────────────────────────────────────────
    def upsert_master_data(self, d: dict) -> dict:
        """Return {'created': bool, 'updated': bool, 'unchanged': bool}."""
        travio_id = d["id"]
        source_payload = json.dumps(d, default=str)
        payload_sha = _sha256(source_payload)

        meta = d.get("_meta") or {}
        contacts = d.get("contacts") or []
        addresses = d.get("addresses") or []

        # Aggregate hashes for change detection
        contact_agg = _sha256(json.dumps(contacts, default=str, sort_keys=True))
        address_agg = _sha256(json.dumps(addresses, default=str, sort_keys=True))

        with self._conn() as conn:
            with _tx(conn) as cur:
                cur.execute(
                    "SELECT payload_sha256 FROM master_data WHERE travio_id = %s",
                    (travio_id,),
                )
                row = cur.fetchone()
                existed = row is not None
                previous_sha = row[0] if row else None

                cur.execute(
                    """
                    INSERT INTO master_data (
                        travio_id, profile_type, honorific_id, legal_form_id,
                        name, surname, company_name, commercial_name, full_name,
                        tax_code, vat_country, vat_number, pec, sdi_code,
                        extra_ue, public_administration,
                        language, nationality, birth_date, birth_place, gender,
                        username, enabled, website,
                        promoter_master_data_id, network_master_data_id,
                        invoice_master_data_id, inbound_payments_master_data_id,
                        outbound_payments_master_data_id,
                        profiles, category_ids, price_lists,
                        source_payload, payload_sha256,
                        contact_count, contact_agg_hash,
                        address_count, address_agg_hash,
                        travio_created_at, travio_last_update,
                        synced_at, updated_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        NOW(), NOW()
                    )
                    ON CONFLICT (travio_id) DO UPDATE SET
                        profile_type                     = EXCLUDED.profile_type,
                        honorific_id                     = EXCLUDED.honorific_id,
                        legal_form_id                    = EXCLUDED.legal_form_id,
                        name                             = EXCLUDED.name,
                        surname                          = EXCLUDED.surname,
                        company_name                     = EXCLUDED.company_name,
                        commercial_name                  = EXCLUDED.commercial_name,
                        full_name                        = EXCLUDED.full_name,
                        tax_code                         = EXCLUDED.tax_code,
                        vat_country                      = EXCLUDED.vat_country,
                        vat_number                       = EXCLUDED.vat_number,
                        pec                              = EXCLUDED.pec,
                        sdi_code                         = EXCLUDED.sdi_code,
                        extra_ue                         = EXCLUDED.extra_ue,
                        public_administration            = EXCLUDED.public_administration,
                        language                         = EXCLUDED.language,
                        nationality                      = EXCLUDED.nationality,
                        birth_date                       = EXCLUDED.birth_date,
                        birth_place                      = EXCLUDED.birth_place,
                        gender                           = EXCLUDED.gender,
                        username                         = EXCLUDED.username,
                        enabled                          = EXCLUDED.enabled,
                        website                          = EXCLUDED.website,
                        promoter_master_data_id          = EXCLUDED.promoter_master_data_id,
                        network_master_data_id           = EXCLUDED.network_master_data_id,
                        invoice_master_data_id           = EXCLUDED.invoice_master_data_id,
                        inbound_payments_master_data_id  = EXCLUDED.inbound_payments_master_data_id,
                        outbound_payments_master_data_id = EXCLUDED.outbound_payments_master_data_id,
                        profiles                         = EXCLUDED.profiles,
                        category_ids                     = EXCLUDED.category_ids,
                        price_lists                      = EXCLUDED.price_lists,
                        source_payload                   = EXCLUDED.source_payload,
                        payload_sha256                   = EXCLUDED.payload_sha256,
                        contact_count                    = EXCLUDED.contact_count,
                        contact_agg_hash                 = EXCLUDED.contact_agg_hash,
                        address_count                    = EXCLUDED.address_count,
                        address_agg_hash                 = EXCLUDED.address_agg_hash,
                        travio_created_at                = EXCLUDED.travio_created_at,
                        travio_last_update               = EXCLUDED.travio_last_update,
                        synced_at                        = NOW(),
                        updated_at                       = NOW()
                    """,
                    (
                        travio_id,
                        d.get("profile_type"),
                        TravioClient.link_id(d.get("honorific")),
                        TravioClient.link_id(d.get("legal_form")),
                        d.get("name"),
                        d.get("surname"),
                        d.get("company_name"),
                        d.get("commercial_name"),
                        d.get("full_name"),
                        d.get("tax_code"),
                        d.get("vat_country"),
                        d.get("vat_number"),
                        d.get("pec"),
                        d.get("sdi_code"),
                        d.get("extra_ue"),
                        d.get("public_administration"),
                        d.get("language"),
                        d.get("nationality"),
                        d.get("birth"),
                        d.get("birth_place"),
                        d.get("gender"),
                        d.get("username"),
                        d.get("enabled"),
                        d.get("website"),
                        TravioClient.link_id(d.get("promoter")),
                        TravioClient.link_id(d.get("network")),
                        TravioClient.link_id(d.get("invoice_master_data")),
                        TravioClient.link_id(d.get("inbound_payments_master_data")),
                        TravioClient.link_id(d.get("outbound_payments_master_data")),
                        json.dumps(d.get("profiles") or []),
                        json.dumps(d.get("categories") or []),
                        json.dumps(d.get("price_lists") or []),
                        source_payload,
                        payload_sha,
                        len(contacts),
                        contact_agg,
                        len(addresses),
                        address_agg,
                        meta.get("creation_date"),
                        meta.get("last_update"),
                    ),
                )

                # Replace contacts
                cur.execute("DELETE FROM master_data_contacts WHERE master_data_travio_id = %s", (travio_id,))
                for ci, contact in enumerate(contacts):
                    contact_payload = json.dumps(contact, default=str)
                    contact_sha = _sha256(contact_payload)
                    cur.execute(
                        """
                        INSERT INTO master_data_contacts (
                            master_data_travio_id, position, travio_contact_id, display_name,
                            phone_values, email_values, fax_values,
                            payload_sha256, source_payload
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            travio_id, ci, contact.get("id"), contact.get("name"),
                            json.dumps(contact.get("phone") or []),
                            json.dumps(contact.get("email") or []),
                            json.dumps(contact.get("fax") or []),
                            contact_sha, contact_payload,
                        ),
                    )

                # Replace addresses
                cur.execute("DELETE FROM master_data_addresses WHERE master_data_travio_id = %s", (travio_id,))
                for ai, addr in enumerate(addresses):
                    addr_payload = json.dumps(addr, default=str)
                    addr_sha = _sha256(addr_payload)
                    legacy = addr.get("legacy") or {}
                    cur.execute(
                        """
                        INSERT INTO master_data_addresses (
                            master_data_travio_id, position, address_type,
                            address_line, postal_code, city, province, region, country,
                            payload_sha256, source_payload
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            travio_id, ai, addr.get("type"),
                            addr.get("address"), addr.get("postal_code"),
                            legacy.get("city"), legacy.get("province"),
                            legacy.get("region"), legacy.get("country"),
                            addr_sha, addr_payload,
                        ),
                    )

                if payload_sha != previous_sha:
                    cur.execute(
                        """
                        INSERT INTO master_data_snapshots (
                            travio_id, payload_sha256, source_payload, captured_at
                        ) VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (travio_id, payload_sha256) DO NOTHING
                        """,
                        (travio_id, payload_sha, source_payload),
                    )

                return {
                    "created": not existed,
                    "updated": existed and payload_sha != previous_sha,
                    "unchanged": existed and payload_sha == previous_sha,
                }

    # ─── helpers ─────────────────────────────────────────────────────────
    @staticmethod
    def _extract_customer(r: dict) -> tuple[str | None, str | None]:
        """Mirror of neon-writer.ts: pax[0] > unfolded client > first_pax."""
        pax = r.get("pax") or []
        if pax:
            first = pax[0]
            n = f"{first.get('name') or ''} {first.get('surname') or ''}".strip()
            if n:
                return n, first.get("email")
        client = r.get("client")
        if isinstance(client, dict):
            n = (client.get("full_name")
                 or f"{client.get('name') or ''} {client.get('surname') or ''}".strip()
                 or None)
            email = None
            for c in client.get("contacts") or []:
                emails = c.get("email") or []
                if emails:
                    email = emails[0]
                    break
            if n:
                return n, email
        return r.get("first_pax"), None

    @staticmethod
    def _latest_status_date(history: list | None) -> str | None:
        if not history:
            return None
        dates = [h.get("date") for h in history if h.get("date")]
        return max(dates) if dates else None
