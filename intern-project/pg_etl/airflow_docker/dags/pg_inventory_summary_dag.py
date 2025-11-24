# pg_inventory_summary_dag.py
# PostgreSQL → ADLS Gen2 (DFS) inventory export (CSV + JSON)
# Artifacts: schemas.csv, tables.csv, columns.csv, primary_keys.csv,
#            indexes.csv, freshness.csv, inventory.json
#
# Required env (typical):
#   PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, PG_SSLMODE=require
#   ADLS_ACCOUNT_NAME=glynacdlgen2
#   ADLS_CONTAINER=pginventory
#   ADLS_ACCOUNT_KEY=<key>         (or ADLS_SAS_TOKEN=?sv=...)
#   # Optionally override the full DFS URL / cloud:
#   # ADLS_ACCOUNT_URL=https://glynacdlgen2.dfs.core.windows.net
#   # ADLS_ENDPOINT_SUFFIX=core.windows.net
#   # ADLS_ROOT_PATH=pg_inventory   (prefix inside the container; default "")

from __future__ import annotations

import os
import re
import json
import pathlib
import datetime as dt
from typing import List, Dict

import pandas as pd
import psycopg2
import psycopg2.extras as pex
from psycopg2 import sql as psql

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook


# ------------------------------- Postgres ---------------------------------- #
def _pg_connect(pg_conn_id: str | None):
    """
    Prefer Airflow Connection; fallback to PG_* environment variables.
    """
    if pg_conn_id:
        try:
            conn = BaseHook.get_connection(pg_conn_id)
            dsn = {
                "host": conn.host,
                "port": conn.port or 5432,
                "dbname": conn.schema or os.getenv("PG_DB"),
                "user": conn.login or os.getenv("PG_USER"),
                "password": conn.password or os.getenv("PG_PASSWORD"),
                "sslmode": (conn.extra_dejson.get("sslmode") if conn.extra else None)
                           or os.getenv("PG_SSLMODE", "require"),
            }
            return psycopg2.connect(**dsn)
        except Exception:
            # fall back to env if the connection isn't present/valid
            pass

    dsn = dict(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "postgres"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )
    missing = [k for k, v in dsn.items() if v in (None, "")]
    if missing:
        raise RuntimeError(
            f"Missing Postgres settings: {missing}. "
            "Provide an Airflow connection or PG_* env vars."
        )
    return psycopg2.connect(**dsn)


# ------------------------------ ADLS Gen2 (DFS) ---------------------------- #
def _adls_clients():
    """
    Build a DataLakeServiceClient & FileSystemClient (container) and return:
      (service_client, filesystem_client, root_path_prefix)
    - Accepts explicit ADLS_ACCOUNT_URL if set (sovereign clouds), else builds
      https://{account}.dfs.{suffix}
    - Ensures the filesystem exists.
    """
    # Lazy import to keep DAG parse cheap if provider not installed yet
    from azure.storage.filedatalake import DataLakeServiceClient

    account = os.getenv("ADLS_ACCOUNT_NAME")
    container = os.getenv("ADLS_CONTAINER", "pginventory")
    root_path = os.getenv("ADLS_ROOT_PATH", "")  # keep empty by default to avoid double prefixes

    if not account or not container:
        raise RuntimeError("ADLS_ACCOUNT_NAME and ADLS_CONTAINER are required.")

    account_url = os.getenv("ADLS_ACCOUNT_URL")
    if not account_url:
        suffix = os.getenv("ADLS_ENDPOINT_SUFFIX", "core.windows.net")
        account_url = f"https://{account}.dfs.{suffix}"

    credential = os.getenv("ADLS_ACCOUNT_KEY") or os.getenv("ADLS_SAS_TOKEN")
    if not credential:
        raise RuntimeError("Provide ADLS_ACCOUNT_KEY or ADLS_SAS_TOKEN.")

    svc = DataLakeServiceClient(account_url=account_url, credential=credential)
    fs = svc.get_file_system_client(file_system=container)

    # Make sure container exists (idempotent)
    try:
        fs.get_file_system_properties()
    except Exception:
        try:
            svc.create_file_system(file_system=container)
        except Exception:
            pass
        fs = svc.get_file_system_client(file_system=container)

    # Normalize root (strip leading/trailing slashes)
    root_path = (root_path or "").strip("/")
    return svc, fs, root_path


def _ensure_dirs(fs, path: str):
    """
    Create directory path recursively (mkdir -p behavior) in ADLS.
    """
    norm = path.strip("/")
    if not norm:
        return
    parts = []
    for part in norm.split("/"):
        parts.append(part)
        dc = fs.get_directory_client("/".join(parts))
        try:
            dc.create_directory()
        except Exception:
            # directory may already exist
            pass


def _upload_dir(local_dir: str, adls_dir_path: str):
    """
    Upload all files from local_dir into ADLS (overwrite=True).
    """
    _, fs, _ = _adls_clients()
    _ensure_dirs(fs, adls_dir_path)

    for p in pathlib.Path(local_dir).glob("*"):
        if p.is_file():
            dest = f"{adls_dir_path}/{p.name}".lstrip("/")
            fc = fs.get_file_client(dest)
            with open(p, "rb") as f:
                fc.upload_data(f.read(), overwrite=True)


# --------------------------------- SQL ------------------------------------- #
SQL_DISCOVER_SCHEMAS = """
SELECT nspname AS schema_name
FROM pg_namespace
WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast')
  AND nspname NOT LIKE 'pg_temp%%'
ORDER BY 1;
"""

SQL_TABLES = """
SELECT n.nspname AS schema_name,
       c.relname AS table_name,
       c.oid     AS table_oid,
       pg_total_relation_size(c.oid) AS total_bytes,
       pg_relation_size(c.oid)       AS table_bytes,
       pg_indexes_size(c.oid)        AS index_bytes,
       COALESCE(s.n_live_tup::bigint, c.reltuples) AS approx_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
WHERE c.relkind = 'r' AND n.nspname = ANY(%s)
ORDER BY 1,2;
"""

SQL_COLUMNS = """
SELECT table_schema AS schema_name, table_name, column_name, ordinal_position,
       data_type, udt_name, is_nullable, column_default,
       character_maximum_length, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema = ANY(%s)
ORDER BY 1,2,4;
"""

SQL_PKS = """
SELECT n.nspname AS schema_name, t.relname AS table_name,
       c.conname AS constraint_name, a.attname AS column_name, cols.ord AS position
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord) ON TRUE
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
WHERE c.contype = 'p' AND n.nspname = ANY(%s)
ORDER BY 1,2,5;
"""

SQL_INDEXES = """
SELECT schemaname AS schema_name, tablename AS table_name, indexname, indexdef
FROM pg_indexes
WHERE schemaname = ANY(%s)
ORDER BY 1,2,3;
"""

SQL_FRESHNESS = """
SELECT n.nspname AS schema_name, c.relname AS table_name,
       s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze,
       s.n_tup_ins, s.n_tup_upd, s.n_tup_del, s.n_dead_tup
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_stat_all_tables s ON s.relid = c.oid
WHERE c.relkind = 'r' AND n.nspname = ANY(%s)
ORDER BY 1,2;
"""


# --------------------------------- DAG ------------------------------------- #
default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="pg_inventory_summary_dag",
    start_date=dt.datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        # include only these schemas (order preserved). Use ["*"] to auto-discover.
        "schema_include": ["public", "airbyte_internal"],
        # exclude via regex patterns applied to "schema.table"
        "exclude_table_patterns": ["^pg_.*", "^information_schema$", "^pg_toast.*", "^pg_temp.*"],
        # exact COUNT(*) only for tables whose total size <= threshold bytes
        "exact_count_threshold_bytes": 50 * 1024 * 1024,  # 50 MB
        # use an Airflow connection or fall back to PG_* env vars
        "pg_conn_id": "postgres_default",
        # subdirectory after ADLS_ROOT_PATH
        "adls_subdir": "pg_inventory",
    },
    doc_md="Profiles PostgreSQL (schemas/tables/columns/PKs/indexes/sizes/rows/"
           "freshness) and uploads CSV+JSON to ADLS Gen2.",
):

    @task
    def collect_and_stage(**ctx) -> Dict[str, str]:
        p = ctx["params"]
        schemas_in: List[str] = p.get("schema_include") or ["*"]
        exclude_res = [re.compile(r) for r in (p.get("exclude_table_patterns") or [])]
        exact_threshold = int(p.get("exact_count_threshold_bytes", 50 * 1024 * 1024))

        tmp_dir = f"/tmp/pg_inventory_{ctx['run_id']}"
        pathlib.Path(tmp_dir).mkdir(parents=True, exist_ok=True)

        with _pg_connect(p.get("pg_conn_id")) as conn:
            conn.autocommit = True
            cur = conn.cursor(cursor_factory=pex.DictCursor)

            cur.execute("SELECT current_database();")
            db_name = cur.fetchone()[0]

            # --- schemas
            cur.execute(SQL_DISCOVER_SCHEMAS)
            discovered = [r["schema_name"] for r in cur.fetchall()]
            if schemas_in == ["*"]:
                schemas = discovered
            else:
                existing = set(discovered)
                schemas = [s for s in schemas_in if s in existing]

            pd.DataFrame({"schema_name": sorted(schemas)}).to_csv(f"{tmp_dir}/schemas.csv", index=False)

            # --- tables (sizes + approx rows)
            cur.execute(SQL_TABLES, (schemas,))
            tables = pd.DataFrame(
                cur.fetchall(),
                columns=[
                    "schema_name",
                    "table_name",
                    "table_oid",
                    "total_bytes",
                    "table_bytes",
                    "index_bytes",
                    "approx_rows",
                ],
            )

            if not tables.empty and exclude_res:
                mask = ~tables.apply(
                    lambda r: any(rx.search(f"{r['schema_name']}.{r['table_name']}") for rx in exclude_res),
                    axis=1,
                )
                tables = tables[mask]

            # --- exact counts for small tables
            row_counts, methods = [], []
            for _, r in tables.iterrows():
                cnt = int(r["approx_rows"]) if r["approx_rows"] is not None else None
                method = "approx"
                if r["total_bytes"] is not None and int(r["total_bytes"]) <= exact_threshold:
                    try:
                        q = psql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                            psql.Identifier(r["schema_name"]),
                            psql.Identifier(r["table_name"]),
                        )
                        cur.execute(q)
                        cnt = int(cur.fetchone()[0])
                        method = "exact"
                    except Exception:
                        # fall back silently
                        pass
                row_counts.append(cnt)
                methods.append(method)

            if not tables.empty:
                tables["row_count"] = row_counts
                tables["row_count_method"] = methods

            tables.to_csv(f"{tmp_dir}/tables.csv", index=False)

            # --- columns
            cur.execute(SQL_COLUMNS, (schemas,))
            cols = pd.DataFrame(
                cur.fetchall(),
                columns=[
                    "schema_name",
                    "table_name",
                    "column_name",
                    "ordinal_position",
                    "data_type",
                    "udt_name",
                    "is_nullable",
                    "column_default",
                    "character_maximum_length",
                    "numeric_precision",
                    "numeric_scale",
                ],
            )
            if not cols.empty and exclude_res:
                mask = ~cols.apply(
                    lambda r: any(rx.search(f"{r['schema_name']}.{r['table_name']}") for rx in exclude_res),
                    axis=1,
                )
                cols = cols[mask]
            cols.to_csv(f"{tmp_dir}/columns.csv", index=False)

            # --- primary keys
            cur.execute(SQL_PKS, (schemas,))
            pks = pd.DataFrame(
                cur.fetchall(),
                columns=["schema_name", "table_name", "constraint_name", "column_name", "position"],
            )
            if not pks.empty and exclude_res:
                mask = ~pks.apply(
                    lambda r: any(rx.search(f"{r['schema_name']}.{r['table_name']}") for rx in exclude_res),
                    axis=1,
                )
                pks = pks[mask]
            pks.to_csv(f"{tmp_dir}/primary_keys.csv", index=False)

            # --- indexes
            cur.execute(SQL_INDEXES, (schemas,))
            idx = pd.DataFrame(cur.fetchall(), columns=["schema_name", "table_name", "indexname", "indexdef"])
            if not idx.empty and exclude_res:
                mask = ~idx.apply(
                    lambda r: any(rx.search(f"{r['schema_name']}.{r['table_name']}") for rx in exclude_res),
                    axis=1,
                )
                idx = idx[mask]
            idx.to_csv(f"{tmp_dir}/indexes.csv", index=False)

            # --- freshness
            cur.execute(SQL_FRESHNESS, (schemas,))
            fr = pd.DataFrame(
                cur.fetchall(),
                columns=[
                    "schema_name",
                    "table_name",
                    "last_vacuum",
                    "last_autovacuum",
                    "last_analyze",
                    "last_autoanalyze",
                    "n_tup_ins",
                    "n_tup_upd",
                    "n_tup_del",
                    "n_dead_tup",
                ],
            )
            if not fr.empty and exclude_res:
                mask = ~fr.apply(
                    lambda r: any(rx.search(f"{r['schema_name']}.{r['table_name']}") for rx in exclude_res),
                    axis=1,
                )
                fr = fr[mask]
            fr.to_csv(f"{tmp_dir}/freshness.csv", index=False)

        # --- consolidated JSON (small summary)
        inv = {
            "db_name": db_name,
            "generated_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "params": {
                "schemas": schemas,
                "exact_count_threshold_bytes": exact_threshold,
                "exclude_table_patterns": [r.pattern for r in exclude_res],
            },
            "counts": {
                "schemas": int(pd.read_csv(f"{tmp_dir}/schemas.csv").shape[0]),
                "tables": int(tables.shape[0]),
                "columns": int(cols.shape[0]),
                "primary_keys": int(pks.shape[0]),
                "indexes": int(idx.shape[0]),
            },
        }
        with open(f"{tmp_dir}/inventory.json", "w", encoding="utf-8") as f:
            json.dump(inv, f, indent=2)

        return {"tmp_dir": tmp_dir, "db_name": db_name}

    @task
    def upload(paths: Dict[str, str], **ctx) -> str:
        """
        Upload staged CSV/JSON to ADLS Gen2 in a date-partitioned path.
        """
        _, _, root = _adls_clients()
        ds, run_id = ctx["ds"], ctx["run_id"]
        sub = (ctx["params"].get("adls_subdir") or "").strip("/")

        # Build: <root>/<sub>/date=YYYY-MM-DD/run_id=<...>/db=<name>
        pieces = [x for x in [root, sub, f"date={ds}", f"run_id={run_id}", f"db={paths['db_name']}"] if x]
        target_prefix = "/".join(pieces)
        _upload_dir(paths["tmp_dir"], target_prefix)
        return target_prefix

    upload(collect_and_stage())
