# pg_top3_widest_validate_cleanse_enrich_load_fabric_dag.py
# Top-3 widest from ADLS -> validate vs Postgres -> cleanse/enrich -> Parquet to ADLS
# Then ensure Fabric table exists (Lakehouse or Warehouse) and load/register.

from __future__ import annotations
import io, os, re, json, base64, datetime as dt, uuid, decimal
from typing import Dict, List, Tuple, Optional

import pandas as pd
import psycopg2, psycopg2.extras as pex
from psycopg2 import sql as psql

from airflow import DAG
from airflow.decorators import task

# ----------------------------- Helpers --------------------------------------

_CONTAINER_RX = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$")

def _sanitize_container_name(name: str) -> str:
    s = name.strip().lower().replace("_", "-")
    s = re.sub(r"[^a-z0-9-]", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    if len(s) < 3: s = (s + "000")[:3]
    if len(s) > 63: s = s[:63]
    s = re.sub(r"^[^a-z0-9]+", "", s)
    s = re.sub(r"[^a-z0-9]+$", "", s)
    if not s or not _CONTAINER_RX.fullmatch(s):
        raise RuntimeError(f"Invalid container name '{name}' (must be 3..63 chars, lowercase, digits, hyphens).")
    if s != name: print(f"[INFO] Normalized container name '{name}' -> '{s}'")
    return s

def _pg_connect():
    dsn = dict(
        host=os.getenv("PG_HOST"), port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB"), user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"), sslmode=os.getenv("PG_SSLMODE", "require"),
    )
    missing = [k for k, v in dsn.items() if not v]
    if missing: raise RuntimeError(f"Missing Postgres settings: {missing}")
    return psycopg2.connect(**dsn)

def _get_datalake_fs(account_url_env: str, container_env: str):
    """Return (service_client, file_system_client) for ADLS Gen2."""
    from azure.storage.filedatalake import DataLakeServiceClient
    url = os.getenv(account_url_env)
    if not url:
        acct = os.getenv("ADLS_ACCOUNT_NAME")
        if not acct: raise RuntimeError(f"Either {account_url_env} or ADLS_ACCOUNT_NAME required.")
        url = f"https://{acct}.dfs.core.windows.net"
    key = os.getenv("ADLS_ACCOUNT_KEY"); sas = os.getenv("ADLS_SAS_TOKEN")
    if key:
        svc = DataLakeServiceClient(account_url=url, credential=key)
    elif sas:
        svc = DataLakeServiceClient(account_url=url + sas)
    else:
        raise RuntimeError("Provide ADLS_ACCOUNT_KEY or ADLS_SAS_TOKEN.")
    container_raw = os.getenv(container_env)
    if not container_raw: raise RuntimeError(f"{container_env} is required.")
    container = _sanitize_container_name(container_raw)
    fs = svc.get_file_system_client(file_system=container)
    try: fs.create_file_system()
    except Exception: pass
    return svc, fs

def _adls_read_csv(fs, path: str) -> pd.DataFrame:
    fc = fs.get_file_client(path); data = fc.download_file().readall()
    return pd.read_csv(io.BytesIO(data))

def _adls_write_bytes(fs, path: str, data: bytes) -> None:
    dir_path = "/".join(path.split("/")[:-1])
    if dir_path:
        d = fs.get_directory_client(dir_path)
        try: d.create_directory()
        except Exception: pass
    fc = fs.get_file_client(path)
    try: fc.create_file()
    except Exception: pass
    fc.upload_data(data, overwrite=True)

def _path_exists(fs, path: str) -> bool:
    try:
        fs.get_file_client(path).get_file_properties(); return True
    except Exception:
        return False

_date_rx = re.compile(r"(?:^|/)date=(\d{4}-\d{2}-\d{2})(?:/|$)")
def _parse_date_from_path(p: str) -> Optional[dt.date]:
    m = _date_rx.search(p); 
    if not m: return None
    try: return dt.datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception: return None

def _normalize_root(root: str) -> str:
    return root.strip("/")

def _resolve_latest_inventory_dir(fs, inv_root: str) -> str:
    inv_root = _normalize_root(inv_root)
    if _path_exists(fs, f"{inv_root}/columns.csv") and _path_exists(fs, f"{inv_root}/primary_keys.csv"):
        return inv_root
    items = list(fs.get_paths(path=inv_root, recursive=True))
    file_names = set(str(it.name) for it in items if not getattr(it, "is_directory", False))
    parents_with_columns = {f.rsplit("/", 1)[0] for f in file_names if f.endswith("/columns.csv") or f == f"{inv_root}/columns.csv"}
    candidates = [p for p in parents_with_columns if f"{p}/primary_keys.csv" in file_names]
    if not candidates:
        raise FileNotFoundError(f"No inventory found under '{inv_root}'. Need 'columns.csv' and 'primary_keys.csv'.")
    def sort_key(p: str) -> Tuple[int, str]:
        d = _parse_date_from_path(p); return (1 if d else 0, "" if not d else d.isoformat())
    return sorted(candidates, key=sort_key)[-1]

# ----------------------------- SQL ------------------------------------------

SQL_LIVE_COLUMNS = """
SELECT c.table_schema, c.table_name, c.column_name, c.ordinal_position,
       c.data_type, c.udt_name, c.is_nullable, c.column_default
FROM information_schema.columns c
WHERE c.table_schema = %s AND c.table_name = %s
ORDER BY c.ordinal_position;
"""
SQL_LIVE_PKS = """
SELECT a.attname AS column_name, ku.ordinality AS position
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
JOIN pg_class t ON t.oid = i.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS ku(attnum, ordinality) ON true
WHERE i.indisprimary AND n.nspname = %s AND t.relname = %s
ORDER BY ku.ordinality;
"""

# -------------------- Cleansing / Normalization -----------------------------

_TRUE = {"true", "t", "y", "yes", "1", 1, True}
_FALSE = {"false", "f", "n", "no", "0", 0, False}

def _trim_strings_inplace(df: pd.DataFrame) -> None:
    obj_cols = [c for c in df.columns if pd.api.types.is_object_dtype(df[c])]
    for c in obj_cols:
        df[c] = df[c].astype(object).where(df[c].notna(), None)
        df[c] = df[c].map(lambda v: v.strip() if isinstance(v, str) else v)

def _normalize_booleans_inplace(df: pd.DataFrame) -> None:
    for c in df.columns:
        if pd.api.types.is_bool_dtype(df[c]):
            continue
        if re.search(r"(?:^|_)(is|has|flag|active|enabled|deleted)(?:_|$)", c, re.I):
            df[c] = df[c].map(lambda v: True if v in _TRUE else (False if v in _FALSE else v))

def _normalize_dataframe_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    def _norm_val(v):
        if v is None or (isinstance(v, float) and pd.isna(v)): return None
        if isinstance(v, (dt.datetime, pd.Timestamp)):
            ts = pd.Timestamp(v)
            try:
                if ts.tz is not None: ts = ts.tz_convert(None)
            except Exception:
                ts = ts.tz_localize(None) if getattr(ts, "tz", None) is not None else ts
            return ts.isoformat()
        if isinstance(v, dt.date): return v.isoformat()
        if isinstance(v, uuid.UUID): return str(v)
        if isinstance(v, decimal.Decimal): return str(v)
        if isinstance(v, (bytes, bytearray, memoryview)): return base64.b64encode(bytes(v)).decode("ascii")
        if isinstance(v, (list, dict)): return json.dumps(v, ensure_ascii=False)
        return v
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.tz_localize(None)
            df[c] = df[c].astype("datetime64[ns]")
        elif pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].map(_norm_val)
        elif pd.api.types.is_bool_dtype(df[c]) or pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].where(df[c].notna(), None).astype(object)
    return df

# ------------------------------- Type mapping (Warehouse) --------------------

def _pg_to_tsql(col_type: str) -> str:
    t = (col_type or "").lower()
    # conservative mapping (safe for COPY)
    if t in ("smallint", "int2"): return "SMALLINT"
    if t in ("integer", "int4", "int"): return "INT"
    if t in ("bigint", "int8"): return "BIGINT"
    if t in ("real", "float4"): return "REAL"
    if t in ("double precision", "float8"): return "FLOAT"
    if t in ("numeric", "decimal"): return "DECIMAL(38,10)"
    if t in ("boolean", "bool"): return "BIT"
    if t in ("date",): return "DATE"
    if "timestamp with" in t: return "DATETIMEOFFSET(6)"
    if "timestamp" in t: return "DATETIME2(6)"
    if t in ("time", "time without time zone"): return "TIME(6)"
    if t in ("bytea",): return "VARBINARY(MAX)"
    if t in ("uuid",): return "UNIQUEIDENTIFIER"
    if t in ("json", "jsonb"): return "NVARCHAR(MAX)"
    # text-ish fallback
    return "NVARCHAR(MAX)"

# ------------------------------- DAG ----------------------------------------

default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="pg_top3_widest_validate_cleanse_enrich_load_fabric_dag",
    start_date=dt.datetime(2024, 1, 1),
    schedule=None, catchup=False, max_active_runs=1,
    default_args=default_args,
    tags=["postgres", "fabric", "adls", "parquet"],
    params={
        "inv_root": os.getenv("INV_ROOT", "pg_inventory"),
        "fabric_root": os.getenv("FABRIC_ROOT", "clean"),
        "source_system": os.getenv("SOURCE_SYSTEM_ID", "postgresql_pg_fabric_src"),
        "sample_rows": int(os.getenv("SAMPLE_ROWS", "200000")),
        "chunk_rows": int(os.getenv("CHUNK_ROWS", "50000")),
        "exclude_table_patterns": [r"^pg_.*\.", r"^information_schema\.", r"^pg_toast.*\.", r"^pg_temp.*\."],
        "fabric_shortcut": os.getenv("FABRIC_SHORTCUT_NAME", "pg-etl-clean"),
        "fabric_target": os.getenv("FABRIC_TARGET", "lakehouse"),  # 'lakehouse' | 'warehouse'
    },
    doc_md="Top-3 widest → validate → cleanse/enrich → Parquet to ADLS → ensure Fabric table exists → load/register."
):

    # 1) Pick Top-3 *widest* tables
    @task
    def pick_top3(**ctx) -> List[Dict]:
        _, inv_fs = _get_datalake_fs("INV_ACCOUNT_URL", "INV_CONTAINER")
        inv_dir = _resolve_latest_inventory_dir(inv_fs, ctx["params"]["inv_root"])
        cols = _adls_read_csv(inv_fs, f"{inv_dir}/columns.csv")
        cols.columns = [c.lower() for c in cols.columns]
        ex = [re.compile(r) for r in (ctx["params"].get("exclude_table_patterns") or [])]
        if ex and not cols.empty:
            cols = cols[~cols.apply(lambda r: any(rx.search(f"{r['schema_name']}.{r['table_name']}") for rx in ex), axis=1)]
        if cols.empty: raise RuntimeError(f"No columns found at '{inv_dir}'.")
        width = (cols.groupby(["schema_name","table_name"], as_index=False)
                   .agg(col_count=("column_name","count"))
                   .sort_values("col_count", ascending=False))
        return width.head(3).to_dict(orient="records")

    # 2) Validate schema + PKs
    @task
    def validate_schema(tbl: Dict, **ctx) -> Dict:
        schema, table = tbl["schema_name"], tbl["table_name"]
        _, inv_fs = _get_datalake_fs("INV_ACCOUNT_URL", "INV_CONTAINER")
        inv_dir = _resolve_latest_inventory_dir(inv_fs, ctx["params"]["inv_root"])
        cols = _adls_read_csv(inv_fs, f"{inv_dir}/columns.csv")
        pks  = _adls_read_csv(inv_fs, f"{inv_dir}/primary_keys.csv")
        cols.columns = [c.lower() for c in cols.columns]; pks.columns = [c.lower() for c in pks.columns]
        inv_cols = cols[(cols.schema_name==schema)&(cols.table_name==table)].sort_values("ordinal_position")
        inv_pk   = pks[(pks.schema_name==schema)&(pks.table_name==table)].sort_values("position")
        with _pg_connect() as conn:
            cur = conn.cursor(cursor_factory=pex.DictCursor)
            cur.execute(SQL_LIVE_COLUMNS, (schema, table))
            live_cols = pd.DataFrame(cur.fetchall(), columns=[
                "table_schema","table_name","column_name","ordinal_position",
                "data_type","udt_name","is_nullable","column_default"
            ])
            cur.execute(SQL_LIVE_PKS, (schema, table))
            live_pks = pd.DataFrame(cur.fetchall(), columns=["column_name","position"]).sort_values("position")
        if set(inv_cols.column_name.str.lower()) != set(live_cols.column_name.str.lower()):
            raise ValueError(f"Schema mismatch {schema}.{table}")
        inv_pk_list  = inv_pk.column_name.str.lower().tolist()
        live_pk_list = live_pks.column_name.str.lower().tolist()
        if inv_pk_list != live_pk_list:
            raise ValueError(f"PK mismatch {schema}.{table}: {inv_pk_list} vs {live_pk_list}")
        # carry inv columns for Warehouse CT
        inv_cols = inv_cols[["column_name","data_type","udt_name"]].reset_index(drop=True)
        return {"schema_name": schema, "table_name": table, "pk": inv_pk_list, "inv_cols": inv_cols.to_dict(orient="records")}

    # 3) Extract → cleanse/enrich → Parquet to ADLS
    @task
    def extract_clean_enrich_and_write(tbl: Dict, **ctx) -> Dict:
        import pyarrow as pa, pyarrow.parquet as pq
        schema, table, pk = tbl["schema_name"], tbl["table_name"], tbl.get("pk", [])
        ds = ctx["ds"]; run_id = re.sub(r"[^A-Za-z0-9_-]", "", ctx["run_id"])
        sample_rows, chunk_rows = int(ctx["params"]["sample_rows"]), int(ctx["params"]["chunk_rows"])
        source_system = ctx["params"]["source_system"]
        _, fab_fs = _get_datalake_fs("FABRIC_ACCOUNT_URL", "FABRIC_CONTAINER")
        fabric_root = ctx["params"]["fabric_root"]
        target_dir = f"{fabric_root}/schema={schema}/table={table}/date={ds}"
        audit_dir  = f"{fabric_root}/_audit/date={ds}/schema={schema}/table={table}"
        total = parts = 0; started_utc = dt.datetime.utcnow().isoformat()

        with _pg_connect() as conn:
            q = psql.SQL("SELECT * FROM {}.{}").format(psql.Identifier(schema), psql.Identifier(table))
            if sample_rows > 0: q = psql.SQL("{} LIMIT {}").format(q, psql.Literal(sample_rows))
            sql_txt = q.as_string(conn)
            for chunk in pd.read_sql_query(sql_txt, conn, chunksize=chunk_rows):
                if chunk.empty: continue
                _trim_strings_inplace(chunk); _normalize_booleans_inplace(chunk)
                if pk: chunk = chunk.drop_duplicates(subset=pk, keep="first")
                df = chunk.copy()
                now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
                df["__ingestion_time"]= now; df["__extraction_time"]= now
                df["__source_system"]= source_system; df["__run_id"]= run_id; df["__table_fqn"]= f"{schema}.{table}"
                df = _normalize_dataframe_for_arrow(df)
                sink = io.BytesIO()
                pq.write_table(pa.Table.from_pandas(df, preserve_index=False), sink, compression="snappy")
                _adls_write_bytes(fab_fs, f"{target_dir}/part-{parts:05d}-{run_id}.parquet", sink.getvalue())
                total += len(df); parts += 1

        if parts == 0: _adls_write_bytes(fab_fs, f"{target_dir}/_SUCCESS", b"")
        audit = {
            "schema": schema, "table": table, "pk": pk,
            "rows_written": int(total), "parts": parts, "path": target_dir,
            "run_id": run_id, "start_utc": started_utc, "end_utc": dt.datetime.utcnow().isoformat(),
            "source_system": source_system,
        }
        _adls_write_bytes(fab_fs, f"{audit_dir}/load_metrics.json", json.dumps(audit, indent=2).encode())
        # carry inv_cols forward for Warehouse CT
        return {**audit, "inv_cols": tbl.get("inv_cols", [])}

    # 4) Ensure Fabric table exists, then load/register
    @task
    def ensure_fabric_table_and_load(info: Dict, **ctx) -> Dict:
        mode = (ctx["params"]["fabric_target"] or "lakehouse").lower()
        schema = info["schema"]; table = info["table"]; ds = ctx["ds"]
        shortcut = ctx["params"]["fabric_shortcut"]
        container = _sanitize_container_name(os.getenv("FABRIC_CONTAINER"))
        account   = os.getenv("ADLS_ACCOUNT_NAME")
        root      = ctx["params"]["fabric_root"]
        folder_abfss = f"abfss://{container}@{account}.dfs.core.windows.net/{root}/schema={schema}/table={table}/date={ds}/"
        # Always write the SQL we plan to run (for transparency / manual run)
        _, fs = _get_datalake_fs("FABRIC_ACCOUNT_URL", "FABRIC_CONTAINER")
        audit_sql_path = f"{root}/_audit/date={ds}/schema={schema}/table={table}/register_or_copy.sql"

        if mode == "lakehouse":
            # Create table USING PARQUET LOCATION 'Files/<shortcut>/...'
            target = f"dbo.[{schema}_{table}]"
            location = f"Files/{shortcut}/{root}/schema={schema}/table={table}/"
            sql = f"""
IF OBJECT_ID('{target}', 'U') IS NULL
BEGIN
  CREATE TABLE {target}
  USING PARQUET
  LOCATION '{location}';
END
"""
            _adls_write_bytes(fs, audit_sql_path, sql.encode("utf-8"))

            server=os.getenv("FABRIC_LH_SQL_SERVER"); db=os.getenv("FABRIC_LH_SQL_DATABASE")
            uid=os.getenv("FABRIC_LH_SQL_UID"); pwd=os.getenv("FABRIC_LH_SQL_PWD")
            executed=False
            if server and db and uid and pwd:
                try:
                    import pytds
                    with pytds.connect(server=server, database=db, user=uid, password=pwd,
                                       port=1433, use_tds7_3=True, autocommit=True,
                                       encrypt=True, validate_host=False) as cn:
                        cn.cursor().execute(sql); executed=True
                except Exception as e:
                    print(f"[WARN] Lakehouse register failed: {e!r}")
            return {"target": target, "mode": mode, "executed": executed, "location": location, "copied_from": None}

        # --- Warehouse path ---
        server=os.getenv("FABRIC_SQL_SERVER"); db=os.getenv("FABRIC_SQL_DATABASE")
        uid=os.getenv("FABRIC_SQL_UID"); pwd=os.getenv("FABRIC_SQL_PWD")
        sas=os.getenv("FABRIC_EXTERNAL_SAS")
        if not all([server, db, uid, pwd, sas]):
            msg="Missing Fabric Warehouse envs; wrote COPY SQL for manual execution."
            # Build a generic COPY script for manual run (creates table if missing, NVARCHAR fallback)
            target = f"dbo.[{schema}_{table}]"
            # build CREATE TABLE columns from inventory (fallback NVARCHAR)
            inv_cols = info.get("inv_cols", []) or []
            col_defs = []
            for c in inv_cols:
                col_name = str(c.get("column_name"))
                col_type = _pg_to_tsql(str(c.get("data_type") or c.get("udt_name") or ""))
                col_defs.append(f"[{col_name}] {col_type} NULL")
            # include enrichment cols
            col_defs += [
                "[__ingestion_time] DATETIMEOFFSET(6) NULL",
                "[__extraction_time] DATETIMEOFFSET(6) NULL",
                "[__source_system] NVARCHAR(200) NULL",
                "[__run_id] NVARCHAR(200) NULL",
                "[__table_fqn] NVARCHAR(400) NULL",
            ]
            create_sql = f"IF OBJECT_ID('{target}','U') IS NULL BEGIN CREATE TABLE {target} ({', '.join(col_defs)}); END\n"
            copy_sql = f"""
{create_sql}
COPY INTO {target}
FROM '{folder_abfss}'
WITH (
  FILE_TYPE = 'PARQUET',
  CREDENTIAL = (IDENTITY='Shared Access Signature', SECRET='{sas}'),
  MAXERRORS = 0
);
"""
            _adls_write_bytes(fs, audit_sql_path, copy_sql.encode("utf-8"))
            return {"target": target, "mode": "warehouse", "executed": False, "copied_from": folder_abfss, "note": msg}

        # If Warehouse envs are present: connect and execute
        import pytds
        target = f"dbo.[{schema}_{table}]"
        inv_cols = info.get("inv_cols", []) or []
        col_defs = []
        for c in inv_cols:
            col_name = str(c.get("column_name"))
            col_type = _pg_to_tsql(str(c.get("data_type") or c.get("udt_name") or ""))
            col_defs.append(f"[{col_name}] {col_type} NULL")
        col_defs += [
            "[__ingestion_time] DATETIMEOFFSET(6) NULL",
            "[__extraction_time] DATETIMEOFFSET(6) NULL",
            "[__source_system] NVARCHAR(200) NULL",
            "[__run_id] NVARCHAR(200) NULL",
            "[__table_fqn] NVARCHAR(400) NULL",
        ]
        create_sql = f"IF OBJECT_ID('{target}','U') IS NULL BEGIN CREATE TABLE {target} ({', '.join(col_defs)}); END\n"
        copy_sql = f"""
{create_sql}
COPY INTO {target}
FROM '{folder_abfss}'
WITH (
  FILE_TYPE = 'PARQUET',
  CREDENTIAL = (IDENTITY='Shared Access Signature', SECRET='{sas}'),
  MAXERRORS = 0
);
"""
        _adls_write_bytes(fs, audit_sql_path, copy_sql.encode("utf-8"))
        executed=False
        try:
            with pytds.connect(server=server, database=db, user=uid, password=pwd,
                               port=1433, use_tds7_3=True, autocommit=True,
                               encrypt=True, validate_host=False) as cn:
                cur = cn.cursor(); cur.execute(copy_sql); executed=True
        except Exception as e:
            print(f"[WARN] Warehouse COPY failed: {e!r}")
        return {"target": target, "mode": "warehouse", "executed": executed, "copied_from": folder_abfss}

    # ----- wiring -----
    top3       = pick_top3()
    validated  = validate_schema.expand(tbl=top3)
    written    = extract_clean_enrich_and_write.expand(tbl=validated)
    ensured    = ensure_fabric_table_and_load.expand(info=written)
    validated >> written >> ensured
