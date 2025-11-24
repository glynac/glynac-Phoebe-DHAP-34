# cleansing_transformation_dag_to_iceberg.py
from __future__ import annotations

import io
import os
import logging
from datetime import datetime
from typing import Dict, List

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.models import DagRun
from azure.storage.blob import BlobServiceClient

# ============
# ENV / CONFIG
# ============
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
RAW_CONTAINER = os.getenv("RAW_CONTAINER", "rawdata")

# Align with Subtask 4 / docker volume
CLEAN_INPUT_DIR = os.getenv("CLEAN_INPUT_DIR", "/opt/airflow/data/staging/clean")
STAGING_DIR = CLEAN_INPUT_DIR

# Optional local map path (not required; we remap from RAW directly)
ID_MAP_FILE = os.path.join(STAGING_DIR, "user_referrals_id_map.csv")

# Input datasets
DATASETS = [
    "user_referrals.csv",
    "user_logs.csv",
    "user_referral_logs.csv",
    "referral_rewards.csv",
    "user_referral_statuses.csv",
    "paid_transactions.csv",
    "lead_log.csv",
]

# ---- Rules (lowercase column names) ----
DTYPE_RULES: Dict[str, Dict[str, str]] = {
    "user_referrals.csv": {
        "referral_at": "datetime",
        "referral_id": "string",
        "referee_id": "string",
        "referee_name": "string",
        "referee_phone": "string",
        "referral_reward_id": "string",
        "referral_source": "string",
        "referrer_id": "string",
        "transaction_id": "string",
        "updated_at": "datetime",
        "user_referral_status_id": "string",
    },
    "user_logs.csv": {
        "id": "string",
        "user_id": "string",
        "name": "string",
        "phone_number": "string",
        "homeclub": "string",
        "timezone_homeclub": "string",
        "membership_expired_date": "date",
        "is_deleted": "bool",
    },
    "user_referral_logs.csv": {
        "id": "string",
        "user_referral_id": "string",
        "source_transaction_id": "string",
        "created_at": "datetime",
        "is_reward_granted": "bool",
    },
    "referral_rewards.csv": {
        "id": "string",
        "reward_value": "float",
        "created_at": "datetime",
        "reward_type": "string",
    },
    "user_referral_statuses.csv": {
        "id": "string",
        "description": "string",
        "created_at": "datetime",
    },
    "paid_transactions.csv": {
        "transaction_id": "string",
        "transaction_status": "string",
        "transaction_at": "datetime",
        "transaction_location": "string",
        "timezone_transaction": "string",
        "transaction_type": "string",
    },
    "lead_log.csv": {
        "id": "string",
        "lead_id": "string",
        "source_category": "string",
        "created_at": "datetime",
        "preferred_location": "string",
        "timezone_location": "string",
        "current_status": "string",
    },
}

# Required columns (drop rows missing any of these)
REQUIRED: Dict[str, List[str]] = {
    "user_referrals.csv": ["referral_id", "referrer_id", "referee_id"],
    "user_logs.csv": ["id", "user_id"],
    "user_referral_logs.csv": ["id", "user_referral_id"],
    "referral_rewards.csv": ["id"],
    "user_referral_statuses.csv": ["id", "description"],
    "paid_transactions.csv": ["transaction_id"],
    "lead_log.csv": ["id", "lead_id"],
}

# Dedup keys
DEDUP_KEYS: Dict[str, List[str]] = {
    "user_referrals.csv": ["referral_id"],
    "user_logs.csv": ["id"],
    "user_referral_logs.csv": ["id"],
    "referral_rewards.csv": ["id"],
    "user_referral_statuses.csv": ["id"],
    "paid_transactions.csv": ["transaction_id"],
    "lead_log.csv": ["id"],
}

# Timezone normalization specs
TZ_RULES: Dict[str, List[Dict[str, str]]] = {
    "user_referrals.csv": [],
    "user_logs.csv": [],
    "user_referral_logs.csv": [],
    "referral_rewards.csv": [],
    "user_referral_statuses.csv": [],
    "paid_transactions.csv": [{"dt": "transaction_at", "tz": "timezone_transaction"}],
    "lead_log.csv": [{"dt": "created_at", "tz": "timezone_location"}],
}

# Optional filters
FILTERS: Dict[str, List[str]] = {
    "user_logs.csv": ["is_deleted != True or is_deleted.isna()"],
    "user_referrals.csv": [],
    "user_referral_logs.csv": [],
    "referral_rewards.csv": [],
    "user_referral_statuses.csv": [],
    "paid_transactions.csv": [],
    "lead_log.csv": [],
}

# --------------- Helpers ---------------

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower()
    return df

def _coerce_series(s: pd.Series, target: str) -> pd.Series:
    t = (target or "").lower()
    if t in ("string", "str", "text"):
        return s.astype("string")
    if t in ("float", "double", "number"):
        return pd.to_numeric(s, errors="coerce").astype("Float64")
    if t in ("int", "integer"):
        return pd.to_numeric(s, errors="coerce").astype("Int64")
    if t in ("bool", "boolean"):
        tmp = s.astype("string").str.strip().str.lower()
        mapped = tmp.map({
            "true": True, "false": False,
            "yes": True,  "no": False,
            "y": True,    "n": False,
            "1": True,    "0": False
        })
        return mapped.astype("boolean")
    if t == "date":
        return pd.to_datetime(s, errors="coerce").dt.date
    if t in ("datetime", "timestamp"):
        return pd.to_datetime(s, errors="coerce")
    return s

def _apply_types(df: pd.DataFrame, rules: Dict[str, str]) -> pd.DataFrame:
    for col, tgt in rules.items():
        if col in df.columns:
            df[col] = _coerce_series(df[col], tgt)
    return df

def _drop_required(df: pd.DataFrame, req: List[str], log: logging.Logger, filename: str) -> pd.DataFrame:
    if not req:
        return df
    cols = [c for c in req if c in df.columns]
    if not cols:
        return df
    # convert blanks and literal 'null'/'none' to NA before dropping
    df[cols] = df[cols].apply(
        lambda s: s.replace({r"^\s*$": pd.NA, r"^(?i)null$": pd.NA, r"^(?i)none$": pd.NA}, regex=True)
    )
    before = len(df)
    df = df.dropna(subset=cols)
    removed = before - len(df)
    if removed:
        log.warning("Dropped %s rows from %s due to missing required columns %s", removed, filename, cols)
    return df

def _apply_filters(df: pd.DataFrame, filters: List[str]) -> pd.DataFrame:
    for expr in filters or []:
        df = df.query(expr)
    return df

def _dedup(df: pd.DataFrame, keys: List[str], log: logging.Logger, filename: str) -> pd.DataFrame:
    keys = [k for k in (keys or []) if k in df.columns]
    if not keys:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=keys, keep="last")
    dups = before - len(df)
    if dups:
        log.info("Removed %s duplicate rows from %s on keys %s", dups, filename, keys)
    return df

def _localize_row_to_utc(row: pd.Series, dt_col: str, tz_col: str):
    val = row.get(dt_col)
    zone = row.get(tz_col)
    if pd.isna(val):
        return pd.NaT
    try:
        ts = pd.to_datetime(val, errors="coerce")
        if pd.isna(ts):
            return pd.NaT
        if ts.tzinfo is None:
            if isinstance(zone, str) and zone.strip():
                try:
                    return ts.tz_localize(zone).tz_convert("UTC")
                except Exception:
                    return ts.tz_localize("UTC")
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")
    except Exception:
        return pd.NaT

def _to_utc(df: pd.DataFrame, tz_specs: List[Dict[str, str]]) -> pd.DataFrame:
    for spec in tz_specs or []:
        dt_col = spec.get("dt")
        tz_col = spec.get("tz")
        if dt_col in df.columns:
            if tz_col and tz_col in df.columns:
                df[dt_col] = df.apply(lambda r: _localize_row_to_utc(r, dt_col, tz_col), axis=1)
            else:
                df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce", utc=True)
    return df

def _trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.select_dtypes(include=["string"]).columns:
        df[c] = df[c].str.strip()
    return df

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

# --------------- Dependency check ---------------

def _ensure_schema_validation_success() -> None:
    runs = DagRun.find(dag_id="schema_validation_dag")
    if not runs:
        raise AirflowSkipException("No runs found for schema_validation_dag yet.")
    latest = sorted(runs, key=lambda r: r.execution_date)[-1]
    if latest.state != "success":
        raise AirflowSkipException(f"Latest schema_validation_dag is {latest.state}, not success.")

# --------------- Main callable ---------------

def clean_transform_all():
    if not AZURE_CONN_STR:
        raise AirflowException("AZURE_STORAGE_CONNECTION_STRING is not set.")
    bsc = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    container = bsc.get_container_client(RAW_CONTAINER)

    _ensure_dir(STAGING_DIR)
    log = logging.getLogger("cleansing_transformation")

    # Preload cleaned paid_transactions (if already present) for FK cleanup
    cleaned_pt_path = os.path.join(STAGING_DIR, "paid_transactions.clean.csv")
    cleaned_pt = None
    if os.path.exists(cleaned_pt_path):
        try:
            cleaned_pt = pd.read_csv(cleaned_pt_path, dtype="string")
        except Exception:
            cleaned_pt = None

    # Optional local id↔referral_id map
    id_map = None
    if os.path.exists(ID_MAP_FILE):
        try:
            tmp = pd.read_csv(ID_MAP_FILE, dtype="string")
            if {"id", "referral_id"}.issubset(tmp.columns):
                id_map = dict(zip(tmp["id"].astype(str), tmp["referral_id"].astype(str)))
        except Exception:
            id_map = None

    # We'll capture the set of cleaned referral_ids to filter logs after remap
    cleaned_user_referrals_ids: set[str] = set()

    for filename in DATASETS:
        # ---- Load RAW from Azure ----
        try:
            blob = container.get_blob_client(filename).download_blob().readall()
            df = pd.read_csv(
                io.BytesIO(blob),
                dtype="string",
                keep_default_na=False,
                na_values=["", "NA", "NaN", "null", "NULL", "None", "NONE"],
            )
        except Exception as e:
            raise AirflowException(f"Error loading {filename} from Azure Blob: {e}")

        before = len(df)
        log.info("Loaded %s rows from %s", before, filename)

        # ---- Clean & transform ----
        df = _normalize_headers(df)                             # lowercase + strip headers
        df = _trim_strings(df)                                  # trim string values
        df = _apply_types(df, DTYPE_RULES.get(filename, {}))    # type coercion
        df = _drop_required(df, REQUIRED.get(filename, []), log, filename)  # drop missing requireds
        df = _apply_filters(df, FILTERS.get(filename, []))      # optional filters
        df = _dedup(df, DEDUP_KEYS.get(filename, []), log, filename)        # dedupe
        df = _to_utc(df, TZ_RULES.get(filename, []))            # timezone normalization

        # ---------- Subtask 3 anomaly handling that unblocks Subtask 4 ----------
        if filename == "referral_rewards.csv":
            if "reward_value" in df.columns:
                missing = df["reward_value"].isna().sum()
                if missing:
                    log.warning("Filling %s null reward_value with 0.0", missing)
                    df["reward_value"] = df["reward_value"].fillna(0.0)

        if filename == "paid_transactions.csv":
            cleaned_pt = df.copy()

        if filename == "user_referrals.csv":
            # Clear transaction_id values not present in cleaned paid_transactions (nullable FK)
            try:
                if cleaned_pt is None and os.path.exists(cleaned_pt_path):
                    cleaned_pt = pd.read_csv(cleaned_pt_path, dtype="string")
                if cleaned_pt is not None and "transaction_id" in df.columns:
                    good = set(cleaned_pt["transaction_id"].astype(str)) if "transaction_id" in cleaned_pt.columns else set()
                    bad_mask = df["transaction_id"].notna() & ~df["transaction_id"].astype(str).isin(good)
                    bad_cnt = int(bad_mask.sum())
                    if bad_cnt:
                        log.warning("Nulling %s invalid transaction_id values in user_referrals", bad_cnt)
                        df.loc[bad_mask, "transaction_id"] = pd.NA
            except Exception as e:
                log.warning("Skipped transaction_id cleanup due to error: %s", e)

            # Record cleaned referral_id keys for downstream log filtering
            if "referral_id" in df.columns:
                cleaned_user_referrals_ids = set(df["referral_id"].astype(str))

        if filename == "user_referral_logs.csv":
            # Remap numeric user_referral_id -> referral_id token using RAW user_referrals
            try:
                raw_blob = container.get_blob_client("user_referrals.csv").download_blob().readall()
                raw_ur = pd.read_csv(io.BytesIO(raw_blob), dtype="string")
                raw_ur.columns = raw_ur.columns.str.strip().str.lower()
                if {"id", "referral_id"}.issubset(raw_ur.columns) and "user_referral_id" in df.columns:
                    id_to_token = dict(zip(raw_ur["id"].astype(str), raw_ur["referral_id"].astype(str)))
                    before_bad = (~df["user_referral_id"].astype(str).isin(raw_ur["referral_id"].astype(str))).sum()
                    df["user_referral_id"] = df["user_referral_id"].astype(str).map(lambda x: id_to_token.get(x, x))
                    after_bad = (~df["user_referral_id"].astype(str).isin(raw_ur["referral_id"].astype(str))).sum()
                    fixed = int(max(0, before_bad - after_bad))
                    if fixed:
                        log.info("Remapped %s user_referral_logs.user_referral_id values to referral_id tokens", fixed)
                else:
                    # fallback to local map if provided
                    if id_map and "user_referral_id" in df.columns:
                        df["user_referral_id"] = df["user_referral_id"].astype(str).map(lambda x: id_map.get(x, x))
            except Exception as e:
                log.warning("Could not remap user_referral_logs via RAW user_referrals: %s", e)

            # Filter logs to only those that reference a CLEANED referral
            if cleaned_user_referrals_ids and "user_referral_id" in df.columns:
                before_logs = len(df)
                df = df[df["user_referral_id"].astype(str).isin(cleaned_user_referrals_ids)].copy()
                dropped = before_logs - len(df)
                if dropped:
                    log.warning("Dropped %s user_referral_logs rows referencing non-existent cleaned referrals", dropped)
        # ------------------------------------------------------------------------

        after = len(df)
        log.info("After cleaning %s: %s -> %s rows", filename, before, after)

        # ---- Write to staging ----
        base = filename.replace(".csv", "")
        out_path = os.path.join(STAGING_DIR, f"{base}.clean.csv")
        df.to_csv(out_path, index=False)

        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise AirflowException(f"Clean output missing/empty: {out_path}")

# --------------- DAG ---------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="cleansing_transformation_dag",
    default_args=default_args,
    description="Subtask 3: Clean & transform CSVs from Azure into local staging (no storage write yet)",
    schedule_interval=None,
    catchup=False,
    tags=["cleansing", "transformation", "azure", "iceberg"],
    max_active_runs=1,
) as dag:

    wait_for_schema_validation = PythonOperator(
        task_id="wait_for_schema_validation",
        python_callable=_ensure_schema_validation_success,
    )

    cleanse_and_transform = PythonOperator(
        task_id="cleanse_and_transform",
        python_callable=clean_transform_all,
    )

    wait_for_schema_validation >> cleanse_and_transform
