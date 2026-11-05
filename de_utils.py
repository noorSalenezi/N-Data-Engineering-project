"""Shared plumbing for the Modern Data Engineering labs.

Concepts are taught in the notebooks; this module only holds the boring, repeated
parts: paths, the DuckDB warehouse, a mock payments API, the Iceberg catalog, dbt
and timing helpers.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import warnings
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

LABS = Path(__file__).resolve().parent
DATA = LABS / "data"
RAW = DATA / "raw"
ARTIFACTS = LABS / "artifacts"        # per-lab outputs (git-ignored)
WAREHOUSE = Path(os.getenv("DE_WAREHOUSE", LABS / "warehouse"))  # shared DuckDB warehouse + Iceberg catalog (git-ignored)
WAREHOUSE_DB = WAREHOUSE / "qafila.duckdb"
ICEBERG = WAREHOUSE / "iceberg"
DBT_PROJECT = LABS / "dbt_qafila"

# Quick mode shrinks workloads (slow laptops, CI).
QUICK = os.getenv("DE_QUICK", "0") == "1"

# Command-line tools (dbt) sit next to the interpreter. A kernel started without the venv activated doesn't have them
# on PATH, and dagster-dbt looks for `dbt` there.
_BIN = str(Path(sys.executable).parent)
if _BIN not in os.environ.get("PATH", "").split(os.pathsep):
    os.environ["PATH"] = _BIN + os.pathsep + os.environ.get("PATH", "")


def pick(full, quick):
    """Return `quick` in quick mode, otherwise `full`."""
    return quick if QUICK else full


def ensure_data() -> Path:
    """Generate the Qafila raw data if it is missing."""
    if not (RAW / "_SUCCESS").exists():
        subprocess.run([sys.executable, str(LABS / "prepare_data.py")], check=True)
    return RAW


def reset_dir(path: Path | str) -> Path:
    """Delete and recreate a directory (only inside labs/artifacts or labs/warehouse)."""
    path = Path(path).resolve()
    if not (path.is_relative_to(ARTIFACTS) or path.is_relative_to(WAREHOUSE)):
        raise ValueError(f"refusing to reset {path}: not under artifacts/ or warehouse/")
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def lab_dir(name: str, reset: bool = False) -> Path:
    """Per-lab output folder under artifacts/."""
    p = ARTIFACTS / name
    return reset_dir(p) if reset else (p.mkdir(parents=True, exist_ok=True) or p)


def size_mb(path: Path | str) -> float:
    """Size of a file, or the total size of a directory, in MB."""
    p = Path(path)
    if p.is_file():
        return p.stat().st_size / 1e6
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) / 1e6


def count_files(path: Path | str, pattern: str = "*.parquet") -> int:
    return sum(1 for _ in Path(path).rglob(pattern))


def peek(path: Path | str, n: int = 3, width: int = 220) -> None:
    """Print the first n lines of a text file, truncated."""
    with open(path, encoding="utf-8-sig") as f:
        for _, line in zip(range(n), f):
            line = line.rstrip("\n")
            print(line if len(line) <= width else line[:width] + " …")


@contextmanager
def timer(label: str = "", results: dict | None = None):
    """with U.timer("csv scan"): ...  prints elapsed seconds (and stores them in `results`)."""
    t0 = time.perf_counter()
    yield
    dt = time.perf_counter() - t0
    if results is not None:
        results[label] = dt
    print(f"{label:<28} {dt * 1000:9.1f} ms")


def best_of(fn, n: int = 3) -> float:
    """Best wall time of n runs, in seconds (less noisy than a single run)."""
    best = float("inf")
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


def check(cond: bool, msg: str) -> None:
    """Print a green tick or a red cross for a lab self-check."""
    print(("✅ " if cond else "❌ ") + msg)


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------

def connect(path: Path | str | None = None, read_only: bool = False):
    """DuckDB connection: in-memory by default, or a database file."""
    import duckdb

    if path is None:
        return duckdb.connect()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def attach_app(con, alias: str = "app") -> None:
    """Attach the Qafila app database (SQLite) read-only under `alias`."""
    ensure_data()
    con.sql("INSTALL sqlite; LOAD sqlite;")
    con.sql(f"ATTACH '{(RAW / 'qafila_oltp.sqlite').as_posix()}' AS {alias} (TYPE sqlite, READ_ONLY)")


def build_warehouse(force: bool = False, path: Path = WAREHOUSE_DB) -> Path:
    """Create the bronze layer (`raw` schema) in the shared DuckDB warehouse.

    This is the end state of the Day 1 ingestion labs, built directly so that Day 2+ labs
    don't depend on having finished them. Tables keep source types and column names.
    """
    import duckdb

    ensure_data()
    if path.exists() and not force:
        con = duckdb.connect(str(path), read_only=True)
        ok = con.sql("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'raw'").fetchall()[0][0] >= 9
        con.close()
        if ok:
            return path
    path.parent.mkdir(parents=True, exist_ok=True)
    for p in (path, Path(str(path) + ".wal")):
        if p.exists():
            p.unlink()
    con = duckdb.connect(str(path))
    raw = RAW.as_posix()
    con.sql("INSTALL sqlite; LOAD sqlite;")
    con.sql(f"ATTACH '{raw}/qafila_oltp.sqlite' AS app (TYPE sqlite, READ_ONLY)")
    con.sql("CREATE SCHEMA raw")
    for t in ["customers", "stores", "products", "orders", "order_items"]:
        con.sql(f"CREATE TABLE raw.{t} AS SELECT *, TIMESTAMP '2026-10-01 00:00:00' AS _loaded_at FROM app.{t}")
    con.sql(f"""CREATE TABLE raw.payments AS
        SELECT * FROM read_json('{raw}/api/payments.jsonl', format='newline_delimited', union_by_name=true,
                                columns={{payment_id: 'VARCHAR', order_id: 'BIGINT', amount: 'DOUBLE', currency: 'VARCHAR',
                                          method: 'VARCHAR', status: 'VARCHAR', card_last4: 'VARCHAR', created_at: 'VARCHAR',
                                          updated_at: 'VARCHAR', wallet_provider: 'VARCHAR'}})""")
    con.sql(f"""CREATE TABLE raw.delivery_events AS
        SELECT * FROM read_json('{raw}/events/delivery_events.jsonl', format='newline_delimited',
                                columns={{event_id: 'VARCHAR', order_id: 'BIGINT', store_id: 'VARCHAR', city: 'VARCHAR',
                                          courier_id: 'VARCHAR', event_type: 'VARCHAR', event_time: 'VARCHAR',
                                          lat: 'DOUBLE', lon: 'DOUBLE', ingest_time: 'VARCHAR'}})""")
    con.sql(f"""CREATE TABLE raw.supplier_prices AS
        SELECT * FROM read_csv('{raw}/suppliers/*.csv', union_by_name=true, all_varchar=true, filename=true)""")
    con.sql(f"CREATE TABLE raw.reviews AS SELECT * FROM read_csv('{raw}/reviews.csv')")
    con.sql(f"CREATE TABLE raw.customers_cdc AS SELECT * FROM read_json_auto('{raw}/cdc/customers_changes.jsonl')")
    con.close()
    return path


# ---------------------------------------------------------------------------
# Mock payments API (Lab 1.3, Day 4)
# ---------------------------------------------------------------------------

class PaymentsAPI:
    """A local, deterministic stand-in for Qafila's payment provider API.

    GET /payments?updated_since=<ISO>&page=<n>&page_size=<n>
        -> {"data": [...], "page": n, "next_page": n+1 | null, "next": "<url>" | null}
    Records are sorted by (updated_at, payment_id); `updated_since` is inclusive.
    Every `rate_limit_every`-th request answers 429 with Retry-After: 1.
    `clock` hides records updated after it, to simulate data arriving over time.
    """

    def __init__(self, page_size: int = 500, rate_limit_every: int = 25, clock: str | None = None):
        ensure_data()
        with open(RAW / "api" / "payments.jsonl", encoding="utf-8") as f:
            self.records = [json.loads(line) for line in f]
        self.page_size = page_size
        self.rate_limit_every = rate_limit_every
        self.clock = clock
        self.requests = 0
        self.throttled = 0
        self._server = None

    def set_clock(self, iso: str | None) -> None:
        """Serve the data as it looked at `iso` (None = everything)."""
        self.clock = iso

    def _as_of(self, clock: str | None) -> list[dict]:
        """Records as of `clock`: a payment refunded later is still 'captured' then."""
        if not clock:
            return self.records
        cache = getattr(self, "_cache", {})
        if clock not in cache:
            rows = []
            for r in self.records:
                if r["updated_at"] > clock:
                    if r["status"] != "refunded" or r["created_at"] > clock:
                        continue
                    r = {k: v for k, v in r.items() if k != "wallet_provider"}
                    r.update(status="captured", updated_at=r["created_at"])
                rows.append(r)
            rows.sort(key=lambda r: (r["updated_at"], r["payment_id"]))
            cache[clock] = rows
            self._cache = cache
        return cache[clock]

    def _visible(self, since: str | None):
        rows = self._as_of(self.clock)
        if since:
            rows = [r for r in rows if r["updated_at"] >= since]
        return rows

    def start(self, port: int = 0) -> str:
        api = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # keep notebooks quiet
                pass

            def _send(self, code: int, body: dict, headers: dict | None = None):
                data = json.dumps(body, ensure_ascii=False).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                url = urlparse(self.path)
                q = {k: v[0] for k, v in parse_qs(url.query).items()}
                api.requests += 1
                if url.path != "/payments":
                    return self._send(404, {"error": "not found"})
                if api.rate_limit_every and api.requests % api.rate_limit_every == 0:
                    api.throttled += 1
                    return self._send(429, {"error": "rate limited"}, {"Retry-After": "1"})
                try:
                    page = int(q.get("page", 1))
                    size = min(int(q.get("page_size", api.page_size)), 1000)
                except ValueError:
                    return self._send(400, {"error": "page and page_size must be integers"})
                rows = api._visible(q.get("updated_since"))
                chunk = rows[(page - 1) * size: page * size]
                more = page * size < len(rows)
                nxt = None
                if more:
                    params = dict(q, page=str(page + 1))
                    nxt = f"{api.base_url}/payments?" + "&".join(f"{k}={v}" for k, v in params.items())
                self._send(200, {"data": chunk, "page": page, "next_page": page + 1 if more else None, "next": nxt})

        self._server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.base_url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self.base_url

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None


# ---------------------------------------------------------------------------
# Iceberg (Day 3, Day 4)
# ---------------------------------------------------------------------------

def get_catalog(root: Path | str = ICEBERG, name: str = "qafila"):
    """A local Iceberg catalog: SQLite for the catalog, the local filesystem as the 'object store'."""
    from pyiceberg.catalog.sql import SqlCatalog

    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    # a plain path (not a file:// URI) keeps Windows drive letters working with PyArrow
    return SqlCatalog(name, uri=f"sqlite:///{(root / 'catalog.db').as_posix()}", warehouse=root.resolve().as_posix())


def _java_major() -> int:
    """Major version of the `java` on PATH / JAVA_HOME (0 if none)."""
    import re

    java = Path(os.environ["JAVA_HOME"]) / "bin" / "java" if os.getenv("JAVA_HOME") else shutil.which("java")
    if not java:
        return 0
    try:
        out = subprocess.run([str(java), "-version"], capture_output=True, text=True).stderr
    except OSError:
        return 0
    m = re.search(r'version "(\d+)', out)
    return int(m.group(1)) if m else 0


def get_spark(app: str = "qafila", cores: int = 2, shuffle_partitions: int = 8):
    """A local SparkSession (Spark 4 needs Java 17+). On Colab, installs Java 17 / PySpark 4 if missing."""
    if "google.colab" in sys.modules:
        if _java_major() < 17:
            print("installing OpenJDK 17 (one-off, ~1 min)...")
            subprocess.run("apt-get install -y -qq openjdk-17-jdk-headless > /dev/null", shell=True, check=True)
            os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
        try:
            import pyspark
            ok = int(pyspark.__version__.split(".")[0]) >= 4
        except ImportError:
            ok = False
        if not ok:
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pyspark>=4.0"], check=True)
            for m in [m for m in sys.modules if m.startswith("pyspark")]:
                del sys.modules[m]
    if _java_major() < 17:
        raise RuntimeError("Spark 4 needs Java 17+: install a JDK (e.g. Temurin 21) and set JAVA_HOME, or run this lab on Colab.")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    warnings.filterwarnings("ignore", message=".*pandas >= 3.*")
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName(app).master(f"local[{cores}]")
             .config("spark.sql.shuffle.partitions", shuffle_partitions)
             .config("spark.sql.session.timeZone", "Asia/Riyadh")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.driver.memory", "2g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def duckdb_iceberg(con) -> None:
    """Load DuckDB's iceberg extension (downloads it on first use)."""
    con.sql("INSTALL iceberg; LOAD iceberg;")


# ---------------------------------------------------------------------------
# dbt (Day 2, Day 4)
# ---------------------------------------------------------------------------

def run_dbt(args: list[str], project_dir: Path | str = DBT_PROJECT, db_path: Path | str = WAREHOUSE_DB, quiet: bool = False):
    """Run a dbt command in-process, e.g. U.run_dbt(["build", "--select", "staging"]).

    Returns dbt's result object; raises if the command failed.
    """
    from dbt.cli.main import dbtRunner

    os.environ["QAFILA_DUCKDB"] = Path(db_path).resolve().as_posix()
    project_dir = Path(project_dir).resolve()
    cli = list(args) + ["--project-dir", str(project_dir), "--profiles-dir", str(project_dir)]
    if quiet:
        cli = ["--quiet"] + cli
    res = dbtRunner().invoke(cli)
    if not res.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed: {res.exception or 'see the log above'}")
    return DbtResult(res)


class DbtResult:
    """dbt's result (.success, .result, .exception), shown as nothing when a cell ends with a dbt call.

    dbtRunnerResult's own repr spells out every node of the run: thousands of lines after a `dbt build`.
    """

    def __init__(self, res):
        self._res = res

    def __getattr__(self, name):
        return getattr(self._res, name)

    def _ipython_display_(self):
        pass



def dbt_lab_copy(out: Path | str) -> tuple[Path, Path]:
    """A private copy of the Qafila dbt project and of the warehouse for one lab: (project_dir, duckdb_file).

    The copy leaves out what Day 3 doesn't teach (incremental config, model contract, unit tests): plain tables and
    tests, the same star. The capstone uses the full project in dbt_qafila/.
    """
    import re

    out = Path(out)
    proj, db = out / "dbt_qafila", out / "qafila.duckdb"
    shutil.copytree(DBT_PROJECT, proj, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    shutil.copy(build_warehouse(), db)

    def edit(rel, fn):
        p = proj / rel
        p.write_text(fn(p.read_text(encoding="utf-8")), encoding="utf-8")

    edit("models/marts/fct_order_items.sql", lambda t: re.sub(
        r"\s*\{%\s*if is_incremental\(\)\s*%\}.*?\{%\s*endif\s*%\}", "",
        re.sub(r"-- Incremental:.*\n", "", re.sub(r"\{\{\s*config\(.*?\)\s*\}\}\s*", "", t, count=1, flags=re.S)), flags=re.S))
    edit("models/marts/_marts.yml", lambda t: re.sub(
        r"\n *constraints:.*|\n *data_type:.*", "", t.replace("    config:\n      contract:\n        enforced: true\n", "")))
    edit("models/staging/_staging.yml", lambda t: t.split("\nunit_tests:")[0].rstrip() + "\n")
    return proj, db
