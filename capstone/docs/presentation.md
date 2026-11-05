# Day 5: slides, demo and Q&A prep

10 minutes talk + live demo, 5 minutes Q&A. Record the demo once beforehand as a backup.

## Slides (7)
1. **Qafila Lakehouse** — team, option 1, one sentence: "from five messy sources to trusted daily sales and delivery numbers, without spreading national IDs."
2. **The platform map** — the course's map (sources → ingest → store → transform → test → serve; orchestrate, observe, govern). For each stage: our tool and why (README, Architecture table).
3. **Sources and what was wrong with them** — the Sources table; highlight 1,040 erased-customer orders, 312 resent events, 16 bad payments, the 15 Sep schema change.
4. **Model and grain** — the star (fct_order_items, fct_orders + 4 dims), the two marts, one line of grain each. Why fees are not on lines (14% overstatement).
5. **Trust** — two blocking gates, the contract, 64 dbt tests (87 pass, 4 warn, 0 errors), quarantine. The real problem found: erased customers → unknown member.
6. **Privacy** — HMAC tokens vs plain hash, the vault, support vs analyst views, the erasure path.
7. **What we'd do next** — CDC for the app tables, streaming SLA alerts, grants instead of schema conventions.

## Live demo (≈4 minutes), from the repository root
```bash
uv run python capstone/pipeline.py --reset --as-of 2026-09-27
```
Point at: payments 39,689 · events 13,506 · cursors printed at the end.
```bash
uv run python capstone/pipeline.py --as-of 2026-09-27
```
"Same day, re-run: still 39,689 payments and 13,506 events. Nothing duplicated."
```bash
uv run python capstone/pipeline.py
```
"Two more days arrive: 2,152 payments (56 of them refunds that replace old rows) and 9,012 events. Totals 41,785 and 22,518."

Then `uv run dagster dev -f capstone/pipeline.py` → open the asset graph, the check on `raw/payments` (quarantine budget), and the SLA alert naming RUH-01.

Optional, 30 seconds, to show the privacy gate block a run: in `ingest.py`, change `if k not in ("national_id", "iban")` to `if k not in ("iban",)`, run once, show that the gate fails and nothing downstream was built, then revert.

## Questions to expect
- **Why DuckDB and not Postgres/Snowflake?** One file, no server, columnar; the data is a few hundred MB. At 100× we'd move raw to Iceberg and transforms to a warehouse engine.
- **Why is the cursor on `ingest_time` and not `event_time`?** A courier offline for 40 minutes sends old events late; a cursor on event time would skip them forever.
- **What if the job dies halfway?** Data and cursor change in one transaction: either both move or neither does. The next run starts from the old cursor.
- **Why not hash the national ID?** Anyone with a leaked ID list can hash it and match. HMAC needs the key.
- **Why keep erased customers' orders?** The sale happened and finance needs it; the person is gone, so the order points to -1 and carries no personal data.
- **Why is the contract a warning but the quarantine budget blocking?** A handful of bad rows is routine and handled (quarantine). More than 1% means the provider broke something, and publishing revenue from it would be wrong.
- **Grain of fct_order_items?** One row per order line with a positive quantity; key `order_line_id`.
