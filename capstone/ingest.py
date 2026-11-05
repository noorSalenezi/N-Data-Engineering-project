"""Ingestion for the Qafila Lakehouse: every source lands in the `raw` schema (bronze) of the warehouse.

Load strategies (see README, "Incremental loads and re-runs"):

    source            type    strategy                                       safe to re-run because
    app tables        DB      full snapshot, replaced in one transaction     the whole table is swapped atomically
    customers         DB      full snapshot, pseudonymized on the way in     same, and IDs never reach the warehouse
    payments          API     incremental on updated_at, upsert by id        cursor + DELETE/INSERT in one transaction
    delivery_events   events  incremental on ingest_time (append)            cursor moves in the same transaction
    supplier_prices   files   full re-read of the weekly drops               replaced atomically
    reviews           file    full re-read                                   replaced atomically

`QAFILA_AS_OF` (YYYY-MM-DD) replays the sources as they looked at the end of that day (UTC), so an
incremental run can be demonstrated: run with --as-of 2026-09-27, re-run it (nothing changes), then
run without it (only the new rows arrive).
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import de_utils as U
import governance as G

RAW = U.RAW.as_posix()
EPOCH = "1970-01-01T00:00:00.000Z"
APP_TABLES = ["stores", "products", "orders", "order_items"]

PAYMENT_COLUMNS = {
    "payment_id": "VARCHAR", "order_id": "BIGINT", "amount": "DOUBLE", "currency": "VARCHAR", "method": "VARCHAR",
    "status": "VARCHAR", "card_last4": "VARCHAR", "created_at": "VARCHAR", "updated_at": "VARCHAR",
    "wallet_provider": "VARCHAR",
}
EVENT_COLUMNS = {
    "event_id": "VARCHAR", "order_id": "BIGINT", "store_id": "VARCHAR", "city": "VARCHAR", "courier_id": "VARCHAR",
    "event_type": "VARCHAR", "event_time": "VARCHAR", "lat": "DOUBLE", "lon": "DOUBLE", "ingest_time": "VARCHAR",
}


def _columns_sql(columns: dict[str, str]) -> str:
    return "{" + ", ".join(f"{k}: '{v}'" for k, v in columns.items()) + "}"


def _table_ddl(columns: dict[str, str]) -> str:
    return ", ".join(f"{k} {v}" for k, v in columns.items()) + ", _loaded_at TIMESTAMP"


def as_of() -> str | None:
    """End of the QAFILA_AS_OF day as an ISO UTC string (same format as the sources), or None for 'now'."""
    day = os.environ.get("QAFILA_AS_OF", "").strip()
    if not day:
        return None
    datetime.strptime(day, "%Y-%m-%d")  # fail early on a typo
    return f"{day}T23:59:59.999Z"


# ---------------------------------------------------------------------------
# Ingestion state: one cursor per incremental source, moved in the same transaction as the data
# ---------------------------------------------------------------------------

def _ensure_state(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("""CREATE TABLE IF NOT EXISTS ops.ingest_state (
                       source VARCHAR, cursor VARCHAR, rows_loaded BIGINT, run_at TIMESTAMP)""")
    con.execute("""CREATE TABLE IF NOT EXISTS ops.ingest_log (
                       source VARCHAR, run_at TIMESTAMP, as_of VARCHAR, cursor_before VARCHAR, cursor_after VARCHAR,
                       rows_fetched BIGINT, rows_in_table BIGINT)""")


def _get_cursor(con, source: str) -> str:
    row = con.execute("SELECT cursor FROM ops.ingest_state WHERE source = ?", [source]).fetchone()
    return row[0] if row else EPOCH


def _set_cursor(con, source: str, cursor: str, rows: int) -> None:
    con.execute("DELETE FROM ops.ingest_state WHERE source = ?", [source])
    con.execute("INSERT INTO ops.ingest_state VALUES (?, ?, ?, now()::TIMESTAMP)", [source, cursor, rows])


def _log(con, source, before, after, fetched, in_table) -> None:
    con.execute("INSERT INTO ops.ingest_log VALUES (?, now()::TIMESTAMP, ?, ?, ?, ?, ?)",
                [source, as_of() or "latest", before, after, fetched, in_table])


def _count(con, table: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


# ---------------------------------------------------------------------------
# App database (SQLite): full snapshots
# ---------------------------------------------------------------------------

def load_app_table(db: Path, table: str) -> int:
    con = U.connect(db)
    try:
        _ensure_state(con)
        con.execute("INSTALL sqlite; LOAD sqlite;")
        con.execute(f"ATTACH '{RAW}/qafila_oltp.sqlite' AS app (TYPE sqlite, READ_ONLY)")
        con.execute("BEGIN")
        con.execute(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT *, now()::TIMESTAMP AS _loaded_at FROM app.{table}")
        con.execute("COMMIT")
        return _count(con, f"raw.{table}")
    finally:
        con.close()


def load_customers(db: Path, vault: Path, landing: Path) -> dict:
    """Customers with the Secret columns taken out before anything reaches the warehouse.

    national_id -> person_token (keyed HMAC), iban -> dropped. The raw values go to the PII vault, a
    separate database file that analytics jobs never open.
    """
    src = sqlite3.connect(U.RAW / "qafila_oltp.sqlite")
    try:
        cols = [r[1] for r in src.execute("PRAGMA table_info(customers)")]
        rows = [dict(zip(cols, r)) for r in src.execute("SELECT * FROM customers")]
    finally:
        src.close()

    landing.mkdir(parents=True, exist_ok=True)
    safe_file = landing / "customers_pseudonymized.jsonl"
    vault_file = landing / "_vault_customers.jsonl"           # deleted below, right after the vault load
    with safe_file.open("w", encoding="utf-8") as f_safe, vault_file.open("w", encoding="utf-8") as f_vault:
        for r in rows:
            token = G.pseudonymize(r["national_id"])
            safe = {k: v for k, v in r.items() if k not in ("national_id", "iban")}
            safe["person_token"] = token
            f_safe.write(json.dumps(safe, ensure_ascii=False) + "\n")
            f_vault.write(json.dumps({"customer_id": r["customer_id"], "person_token": token,
                                      "national_id": r["national_id"], "iban": r["iban"]}) + "\n")

    customer_cols = {
        "customer_id": "BIGINT", "full_name": "VARCHAR", "email": "VARCHAR", "mobile": "VARCHAR", "city": "VARCHAR",
        "district": "VARCHAR", "created_at": "VARCHAR", "updated_at": "VARCHAR", "is_deleted": "BIGINT",
        "person_token": "VARCHAR",
    }
    vault_cols = {"customer_id": "BIGINT", "person_token": "VARCHAR", "national_id": "VARCHAR", "iban": "VARCHAR"}

    try:
        v = U.connect(vault)
        try:
            v.execute("CREATE SCHEMA IF NOT EXISTS pii")
            v.execute(f"""CREATE OR REPLACE TABLE pii.customer_identifiers AS
                          SELECT * FROM read_json('{vault_file.as_posix()}', format='newline_delimited',
                                                  columns={_columns_sql(vault_cols)})""")
        finally:
            v.close()
    finally:
        vault_file.unlink(missing_ok=True)

    con = U.connect(db)
    try:
        _ensure_state(con)
        con.execute("BEGIN")
        con.execute(f"""CREATE OR REPLACE TABLE raw.customers AS
                        SELECT *, now()::TIMESTAMP AS _loaded_at
                        FROM read_json('{safe_file.as_posix()}', format='newline_delimited',
                                       columns={_columns_sql(customer_cols)})""")
        con.execute("COMMIT")
        n = _count(con, "raw.customers")
        people = con.execute("SELECT count(DISTINCT person_token) FROM raw.customers").fetchone()[0]
    finally:
        con.close()
    return {"rows": n, "people": people, "dev_key": G.USING_DEV_KEY}


# ---------------------------------------------------------------------------
# Payments API: incremental on updated_at, upsert by payment_id
# ---------------------------------------------------------------------------

def _get_with_retry(url: str, params: dict, tries: int = 6) -> dict:
    import requests

    for attempt in range(tries):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429:                      # the provider rate-limits: wait as told, then retry
            time.sleep(float(r.headers.get("Retry-After", 2 ** attempt)))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"payments API: gave up after {tries} tries")


def _fetch_payments(since: str, clock: str | None) -> tuple[list[dict], dict]:
    """The mock provider from the labs (U.PaymentsAPI), started for this run and stopped after it."""
    api = U.PaymentsAPI(page_size=1000, rate_limit_every=25, clock=clock)
    base = api.start()
    try:
        rows, page = [], 1
        while page:
            body = _get_with_retry(f"{base}/payments", {"updated_since": since, "page": page, "page_size": 1000})
            rows += body["data"]
            page = body["next_page"]
        return rows, {"requests": api.requests, "rate_limited": api.throttled}
    finally:
        api.stop()


def load_payments(db: Path, landing: Path) -> dict:
    con = U.connect(db)
    try:
        _ensure_state(con)
        con.execute(f"CREATE TABLE IF NOT EXISTS raw.payments ({_table_ddl(PAYMENT_COLUMNS)})")
        before = _get_cursor(con, "payments")
        rows, stats = _fetch_payments(before, as_of())       # updated_since is inclusive: the boundary row comes back
        if not rows:
            _log(con, "payments", before, before, 0, _count(con, "raw.payments"))
            return {"fetched": 0, "rows_in_table": _count(con, "raw.payments"), "cursor": before, **stats}

        # Land the batch as a file first: an audit trail of exactly what the provider sent, and a typed read.
        landing.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        batch_file = landing / f"payments_{stamp}.jsonl"
        with batch_file.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        after = max(r["updated_at"] for r in rows)

        con.execute("BEGIN")
        try:
            con.execute(f"""CREATE OR REPLACE TEMP TABLE batch AS
                            SELECT * FROM read_json('{batch_file.as_posix()}', format='newline_delimited',
                                                    columns={_columns_sql(PAYMENT_COLUMNS)})""")
            # Upsert: a refund arrives as a new version of an existing payment; it replaces the old row.
            con.execute("DELETE FROM raw.payments WHERE payment_id IN (SELECT payment_id FROM batch)")
            con.execute(f"INSERT INTO raw.payments SELECT {', '.join(PAYMENT_COLUMNS)}, now()::TIMESTAMP FROM batch")
            _set_cursor(con, "payments", after, len(rows))
            _log(con, "payments", before, after, len(rows), _count(con, "raw.payments"))
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        total, ids = con.execute("SELECT count(*), count(DISTINCT payment_id) FROM raw.payments").fetchone()
        return {"fetched": len(rows), "rows_in_table": total, "distinct_ids": ids, "cursor": after, **stats}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Courier events: incremental on ingest_time (arrival), append-only bronze
# ---------------------------------------------------------------------------

def load_delivery_events(db: Path) -> dict:
    """Append what arrived since the last run. The cursor is on ingest_time (when the platform received the
    event), not event_time: a courier who was offline sends old events late, and those must still be picked up.
    Phones that resend an event create true duplicates in the log; bronze keeps them as received and staging
    drops them (first arrival wins)."""
    con = U.connect(db)
    try:
        _ensure_state(con)
        con.execute(f"CREATE TABLE IF NOT EXISTS raw.delivery_events ({_table_ddl(EVENT_COLUMNS)})")
        before = _get_cursor(con, "delivery_events")
        upper = as_of()
        src = (f"read_json('{RAW}/events/delivery_events.jsonl', format='newline_delimited', "
               f"columns={_columns_sql(EVENT_COLUMNS)})")
        # cursor values are ISO timestamps we wrote ourselves (validated by as_of()), so inlining them is safe
        where = f"ingest_time > '{before}'" + (f" AND ingest_time <= '{upper}'" if upper else "")

        con.execute("BEGIN")
        try:
            con.execute(f"CREATE OR REPLACE TEMP TABLE batch AS SELECT * FROM {src} WHERE {where}")
            fetched, after = con.execute("SELECT count(*), max(ingest_time) FROM batch").fetchone()
            if fetched:
                con.execute(f"INSERT INTO raw.delivery_events SELECT {', '.join(EVENT_COLUMNS)}, now()::TIMESTAMP FROM batch")
                _set_cursor(con, "delivery_events", after, fetched)
            after = after or before
            _log(con, "delivery_events", before, after, fetched, _count(con, "raw.delivery_events"))
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        total, dup = con.execute("SELECT count(*), count(*) - count(DISTINCT event_id) FROM raw.delivery_events").fetchone()
        return {"fetched": fetched, "rows_in_table": total, "resent_by_phones": dup, "cursor": after}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Files: weekly supplier drops and the reviews export (small, re-read in full)
# ---------------------------------------------------------------------------

FILE_SOURCES = {
    "supplier_prices": f"read_csv('{RAW}/suppliers/*.csv', union_by_name=true, all_varchar=true, filename=true)",
    "reviews": f"read_csv('{RAW}/reviews.csv')",
}


def load_file_source(db: Path, table: str) -> int:
    con = U.connect(db)
    try:
        _ensure_state(con)
        con.execute("BEGIN")
        con.execute(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT *, now()::TIMESTAMP AS _loaded_at FROM {FILE_SOURCES[table]}")
        con.execute("COMMIT")
        return _count(con, f"raw.{table}")
    finally:
        con.close()
