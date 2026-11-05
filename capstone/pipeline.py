"""Qafila Lakehouse: the whole platform as one Dagster code location.

Sources -> raw (DuckDB, bronze) -> dbt (staging -> marts, quarantine, role views) -> gold on Iceberg,
with blocking quality and privacy gates, a data contract, a volume monitor and a nightly schedule.

Run from the repository root (one command, end to end):

    uv run python capstone/pipeline.py                        # everything, as of today
    uv run python capstone/pipeline.py --as-of 2026-09-27     # replay the sources as of a past day
    uv run python capstone/pipeline.py --reset                # start from an empty warehouse
    uv run dagster dev -f capstone/pipeline.py                # the UI: lineage, runs, checks, schedule

Incremental demo: --reset --as-of 2026-09-27, then the same again without --reset (nothing new arrives),
then no flags (only payments and events after 27 Sep arrive).
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
HERE = Path(__file__).resolve().parent
LABS = HERE.parent
sys.path[:0] = [str(LABS), str(HERE)]

import dagster as dg  # noqa: E402
import yaml  # noqa: E402
from dagster_dbt import DbtCliResource, dbt_assets  # noqa: E402

import de_utils as U  # noqa: E402
import governance as G  # noqa: E402
import ingest as I  # noqa: E402

HOME = U.ARTIFACTS / "capstone"             # git-ignored: data never goes to GitHub
DB = HOME / "qafila.duckdb"                 # the warehouse (raw, staging, marts, quarantine, support, analyst)
VAULT = HOME / "vault" / "pii_vault.duckdb" # national IDs and IBANs; analytics jobs never open it
LANDING = HOME / "landing"                  # exactly what each API call returned, for audit and replay
LAKE = HOME / "lakehouse"                   # gold tables as Iceberg
DBT_DIR = HERE / "dbt_project"
CONTRACT = HERE / "contracts" / "payments.odcs.yaml"
HOME.mkdir(parents=True, exist_ok=True)
os.environ["QAFILA_DUCKDB"] = DB.as_posix()  # dbt's profiles.yml reads this


def sql(query: str, params: list | None = None):
    con = U.connect(DB)
    con.execute("SET python_enable_replacements = false")
    try:
        return con.execute(query, params or []).fetchall()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# 1. Ingestion (asset keys match the dbt sources: raw.<table>)
# ---------------------------------------------------------------------------
def _make_app_asset(table: str):
    @dg.asset(key=["raw", table], group_name="ingest", kinds={"sqlite", "duckdb"},
              description=f"App database: {table} (full snapshot, swapped atomically)")
    def _asset():
        return dg.MaterializeResult(metadata={"rows": I.load_app_table(DB, table), "strategy": "full snapshot"})
    return _asset


app_assets = [_make_app_asset(t) for t in I.APP_TABLES]


@dg.asset(key=["raw", "customers"], group_name="ingest", kinds={"sqlite", "duckdb"},
          description="Customers, pseudonymized on the way in: national_id -> person_token, iban dropped (PII vault).")
def raw_customers():
    stats = I.load_customers(DB, VAULT, LANDING)
    return dg.MaterializeResult(metadata={**stats, "strategy": "full snapshot + pseudonymization"})


@dg.asset(key=["raw", "payments"], group_name="ingest", kinds={"api", "duckdb"},
          description="Payment provider API: incremental on updated_at, upsert by payment_id.",
          retry_policy=dg.RetryPolicy(max_retries=2, delay=5))
def raw_payments():
    return dg.MaterializeResult(metadata={**I.load_payments(DB, LANDING / "payments"), "strategy": "incremental upsert"})


@dg.asset(key=["raw", "delivery_events"], group_name="ingest", kinds={"duckdb"},
          description="Courier events: incremental on ingest_time (append-only bronze; staging dedupes).")
def raw_delivery_events():
    return dg.MaterializeResult(metadata={**I.load_delivery_events(DB), "strategy": "incremental append"})


def _make_file_asset(table: str):
    @dg.asset(key=["raw", table], group_name="ingest", kinds={"duckdb"}, description=f"Files: {table} (full re-read)")
    def _asset():
        return dg.MaterializeResult(metadata={"rows": I.load_file_source(DB, table), "strategy": "full re-read"})
    return _asset


file_assets = [_make_file_asset(t) for t in I.FILE_SOURCES]
ingest_assets = [*app_assets, raw_customers, raw_payments, raw_delivery_events, *file_assets]


# ---------------------------------------------------------------------------
# 2. Gates on raw data: blocking checks stop everything downstream; the contract warns
# ---------------------------------------------------------------------------
@dg.asset_check(asset=raw_customers, blocking=True,
                description="Privacy gate: no national ID or IBAN may reach the warehouse")
def no_identifiers_in_raw_customers():
    cols = {c for (c,) in sql("SELECT column_name FROM information_schema.columns "
                              "WHERE table_schema = 'raw' AND table_name = 'customers'")}
    forbidden_cols = sorted(cols & {"national_id", "iban"})
    text_cols = [c for c in ("full_name", "email", "mobile") if c in cols]
    leaked = sql("SELECT count(*) FROM raw.customers WHERE " +
                 " OR ".join(f"regexp_matches({c}, '(^|[^0-9])[12][0-9]{{9}}([^0-9]|$)')" for c in text_cols))[0][0]
    return dg.AssetCheckResult(passed=not forbidden_cols and leaked == 0,
                               metadata={"forbidden_columns": ", ".join(forbidden_cols) or "none",
                                         "values_that_look_like_ids": leaked})


@dg.asset_check(asset=raw_payments, blocking=True,
                description="Quarantine budget: at most 1% of payment rows may break the contract")
def payments_quarantine_budget():
    bad, total = sql("""SELECT count(*) FILTER (WHERE NOT coalesce(amount > 0 AND currency = 'SAR'
                               AND status IN ('captured', 'failed', 'refunded')
                               AND method IN ('card', 'apple_pay', 'wallet'), false)),
                               count(*)
                        FROM raw.payments""")[0]
    rate = bad / total if total else 0.0
    return dg.AssetCheckResult(passed=total > 0 and rate <= 0.01,
                               metadata={"rows": total, "to_quarantine": bad, "rate_pct": round(100 * rate, 3)})


@dg.asset_check(asset=raw_payments, description="ODCS contract: capstone/contracts/payments.odcs.yaml (warns)")
def payments_contract():
    contract = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    obj = contract["schema"][0]
    failures, checked = [], 0
    rules = [(None, r) for r in obj.get("quality", [])] + [(p["name"], r) for p in obj["properties"] for r in p.get("quality", [])]
    for col, rule in rules:
        if rule.get("type") == "sql":
            value = sql(rule["query"].replace("{object}", "raw.payments"))[0][0]
        elif rule["metric"] == "rowCount":
            value = sql("SELECT count(*) FROM raw.payments")[0][0]
        elif rule["metric"] == "duplicateValues":
            cols = ", ".join(rule["arguments"]["properties"])
            value = sql(f"SELECT count(*) - count(DISTINCT ({cols})) FROM raw.payments")[0][0]
        elif rule["metric"] == "invalidValues" and "validValues" in rule["arguments"]:
            allowed = ", ".join(f"'{v}'" for v in rule["arguments"]["validValues"])
            value = sql(f"SELECT count(*) FROM raw.payments WHERE {col} IS NOT NULL AND {col} NOT IN ({allowed})")[0][0]
        elif rule["metric"] == "invalidValues":
            value = sql(f"SELECT count(*) FROM raw.payments WHERE {col} IS NOT NULL AND NOT regexp_full_match({col}, ?)",
                        [rule["arguments"]["pattern"]])[0][0]
        else:
            continue
        checked += 1
        op, target = next((k, v) for k, v in rule.items() if k.startswith("must"))
        ok = {"mustBe": value == target, "mustBeGreaterThan": value > target, "mustBeLessThan": value < target}[op]
        if not ok:
            failures.append(f"{rule.get('id', col)}: {value} ({op} {target})")
    return dg.AssetCheckResult(passed=not failures, severity=dg.AssetCheckSeverity.WARN,
                               metadata={"rules_checked": checked, "failures": "; ".join(failures) or "none",
                                         "handled_by": "rows that fail go to quarantine.qrn_payments"})


# ---------------------------------------------------------------------------
# 3. dbt: staging, intermediate, marts, quarantine, serve (every dbt test becomes an asset check)
# ---------------------------------------------------------------------------
MANIFEST = DBT_DIR / "target" / "manifest.json"


def _manifest_is_stale() -> bool:
    if not MANIFEST.exists():
        return True
    newest = max(p.stat().st_mtime for p in DBT_DIR.rglob("*") if p.suffix in {".sql", ".yml", ".csv"}
                 and "target" not in p.parts)
    return newest > MANIFEST.stat().st_mtime


if _manifest_is_stale():
    U.run_dbt(["parse"], project_dir=DBT_DIR, db_path=DB, quiet=True)


@dbt_assets(manifest=MANIFEST)
def qafila_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# ---------------------------------------------------------------------------
# 4. Gold: publish marts to Iceberg so any engine can read them; monitor volume
# ---------------------------------------------------------------------------
def _publish(mart: str, name: str, order_by: str) -> dict:
    con = U.connect(DB, read_only=True)
    try:
        data = con.sql(f"SELECT * FROM marts.{mart} ORDER BY {order_by}").to_arrow_table()
    finally:
        con.close()
    catalog = U.get_catalog(LAKE)
    catalog.create_namespace_if_not_exists("gold")
    ident = f"gold.{name}"
    if catalog.table_exists(ident):
        table = catalog.load_table(ident)
        table.overwrite(data)            # one atomic commit: readers see the old or the new version, never half
    else:
        table = catalog.create_table(ident, schema=data.schema)
        table.append(data)
    return {"rows": data.num_rows, "snapshots": len(table.snapshots()), "metadata_location": table.metadata_location}


@dg.asset(key=["gold", "daily_city_sales"], group_name="publish", kinds={"iceberg"},
          deps=[dg.AssetKey(["marts", "mart_daily_city_sales"])])
def gold_daily_city_sales():
    """Qafila's daily dashboard (one row per Riyadh day and city), as an Iceberg table."""
    return dg.MaterializeResult(metadata=_publish("mart_daily_city_sales", "daily_city_sales", "order_date, city"))


@dg.asset(key=["gold", "store_delivery_ops"], group_name="publish", kinds={"iceberg"},
          deps=[dg.AssetKey(["marts", "mart_store_delivery_ops"])])
def gold_store_delivery_ops():
    """The operations view (one row per store and Riyadh day), as an Iceberg table."""
    return dg.MaterializeResult(metadata=_publish("mart_store_delivery_ops", "store_delivery_ops", "order_date, store_id"))


@dg.asset_check(asset=dg.AssetKey(["marts", "fct_orders"]),
                description="Volume: no day below 70% of its weekday's 4-week median (warns)")
def orders_volume():
    rows = sql("""
        WITH d AS (SELECT order_date, count(*)::DOUBLE AS n FROM marts.fct_orders GROUP BY 1)
        SELECT order_date, n,
               (SELECT median(n) FROM d d2 WHERE d2.order_date < d.order_date AND d2.order_date >= d.order_date - 28
                  AND dayofweek(d2.order_date) = dayofweek(d.order_date)) AS expected
        FROM d ORDER BY order_date""")
    low = [str(day) for day, n, exp in rows if exp and n < 0.7 * exp]
    return dg.AssetCheckResult(passed=not low, severity=dg.AssetCheckSeverity.WARN,
                               metadata={"days_checked": len(rows), "low_days": ", ".join(low) or "none"})


@dg.asset_check(asset=dg.AssetKey(["marts", "mart_store_delivery_ops"]),
                description="SLA alert: a store whose latest day delivers under half its orders within 30 minutes (warns)")
def store_sla_alert():
    rows = sql("""SELECT store_id, sla_rate FROM marts.mart_store_delivery_ops
                  WHERE order_date = (SELECT max(order_date) FROM marts.mart_store_delivery_ops)
                    AND delivered_orders >= 10 AND sla_rate < 0.5 ORDER BY sla_rate""")
    return dg.AssetCheckResult(passed=not rows, severity=dg.AssetCheckSeverity.WARN,
                               metadata={"stores_below_50pct": ", ".join(f"{s} ({r:.0%})" for s, r in rows) or "none"})


# ---------------------------------------------------------------------------
# 5. The code location. One process at a time: DuckDB has a single writer.
# ---------------------------------------------------------------------------
ALL_ASSETS = [*ingest_assets, qafila_dbt, gold_daily_city_sales, gold_store_delivery_ops]
ALL_CHECKS = [no_identifiers_in_raw_customers, payments_quarantine_budget, payments_contract, orders_volume, store_sla_alert]
RESOURCES = {"dbt": DbtCliResource(project_dir=str(DBT_DIR), profiles_dir=str(DBT_DIR))}

refresh_all = dg.define_asset_job("refresh_all", selection=dg.AssetSelection.all(), executor_def=dg.in_process_executor)
defs = dg.Definitions(
    assets=ALL_ASSETS,
    asset_checks=ALL_CHECKS,
    jobs=[refresh_all],
    schedules=[dg.ScheduleDefinition(job=refresh_all, cron_schedule="30 1 * * *", execution_timezone="Asia/Riyadh")],
    resources=RESOURCES,
    executor=dg.in_process_executor,
)


def _summary() -> None:
    q = lambda s: sql(s)[0][0]  # noqa: E731
    print("\nwarehouse")
    for source, cursor, rows, run_at in sql("SELECT * FROM ops.ingest_state ORDER BY source"):
        print(f"  cursor {source:16} {cursor}  (last batch {rows:,} rows)")
    print(f"  raw.payments          {q('SELECT count(*) FROM raw.payments'):>8,} rows")
    print(f"  raw.delivery_events   {q('SELECT count(*) FROM raw.delivery_events'):>8,} rows")
    for t in ["marts.fct_orders", "marts.fct_order_items", "marts.mart_daily_city_sales", "marts.mart_store_delivery_ops",
              "quarantine.qrn_payments", "quarantine.qrn_order_items"]:
        try:
            print(f"  {t:28} {q(f'SELECT count(*) FROM {t}'):>8,} rows")
        except Exception:
            print(f"  {t:28}  (not built)")
    print(f"warehouse: {DB}\nPII vault: {VAULT}\nlakehouse: {LAKE}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the Qafila Lakehouse once, end to end.")
    ap.add_argument("--as-of", help="replay the sources as they looked at the end of this day (YYYY-MM-DD)")
    ap.add_argument("--reset", action="store_true", help="delete the warehouse, vault and lakehouse first")
    args = ap.parse_args()

    sys.stdout.reconfigure(errors="replace")  # Windows consoles with an Arabic code page
    if args.as_of:
        os.environ["QAFILA_AS_OF"] = args.as_of
    else:
        os.environ.pop("QAFILA_AS_OF", None)
    if args.reset:
        U.reset_dir(HOME)
    if G.USING_DEV_KEY:
        print("note: using the development pseudonymization key; set QAFILA_PSEUDO_KEY for anything real")
    U.ensure_data()

    result = dg.materialize(ALL_ASSETS + ALL_CHECKS, resources=RESOURCES, raise_on_error=False,
                            run_config={"loggers": {"console": {"config": {"log_level": "ERROR"}}}})
    mats = result.get_asset_materialization_events()
    checks = result.get_asset_check_evaluations()
    print(f"\nrun {'succeeded' if result.success else 'FAILED'} (as of {args.as_of or 'latest'}): "
          f"{len(mats)} assets materialized, {len(checks)} checks, {sum(not c.passed for c in checks)} not passed")
    for m in mats:
        key = m.asset_key.to_user_string()
        if key.startswith("raw/"):
            meta = {k: v.value for k, v in m.step_materialization_data.materialization.metadata.items()}
            shown = {k: meta[k] for k in ("rows", "fetched", "rows_in_table", "people", "resent_by_phones", "rate_limited") if k in meta}
            print(f"  {key:22} {shown}")
    for c in checks:
        if not c.passed:
            meta = {k: v.value for k, v in c.metadata.items()}
            detail = (meta.get("failures") or meta.get("low_days") or meta.get("stores_below_50pct")
                      or f"{meta.get('dagster_dbt/failed_row_count', '?')} failing rows")
            print(f"  ⚠️  {c.asset_key.to_user_string()} · {c.check_name}: {detail}")
    try:
        _summary()
    except Exception as e:  # the warehouse may be half-built if a blocking gate stopped the run
        print(f"(summary unavailable: {e})")
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
