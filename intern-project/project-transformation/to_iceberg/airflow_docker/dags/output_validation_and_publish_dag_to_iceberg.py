# dags/output_validation_and_publish_dag_to_iceberg.py
from __future__ import annotations
import json, os
from datetime import datetime
from typing import Dict, List

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount

# ---- Config from env ----
CLEAN_INPUT_DIR = os.getenv("CLEAN_INPUT_DIR", "/opt/airflow/data/staging/clean")
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "overwrite").lower()
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "lake")

# Absolute host paths (not container paths)
HOST_DAGS_DIR  = os.getenv("HOST_DAGS_DIR")    # e.g., /abs/path/to/your/project/dags
HOST_CLEAN_DIR = os.getenv("HOST_CLEAN_DIR")   # e.g., /abs/path/to/your/project/data/staging/clean

REPORT_DIR = "/opt/airflow/logs/data_quality"
REPORT_JSON = os.path.join(REPORT_DIR, "output_validation_report.json")
REPORT_CSV = os.path.join(REPORT_DIR, "output_validation_report.csv")

FILES = {
    "user_referrals": "user_referrals.clean.csv",
    "user_logs": "user_logs.clean.csv",
    "user_referral_logs": "user_referral_logs.clean.csv",
    "referral_rewards": "referral_rewards.clean.csv",
    "user_referral_statuses": "user_referral_statuses.clean.csv",
    "paid_transactions": "paid_transactions.clean.csv",
    "lead_log": "lead_log.clean.csv",
}

RULES = {
    "user_referrals": {
        "pk": ["referral_id"],
        "not_null": ["referral_id", "referee_id", "user_referral_status_id", "referral_at"],
        "fk": [
            ("referral_reward_id", "referral_rewards", "id", True),
            ("user_referral_status_id", "user_referral_statuses", "id", False),
            ("transaction_id", "paid_transactions", "transaction_id", True),
        ],
        "ts": "referral_at",
    },
    "user_logs": {"pk": ["id"], "not_null": ["id", "user_id"], "fk": [], "ts": None},
    "user_referral_logs": {
        "pk": ["id"],
        "not_null": ["id", "user_referral_id", "created_at"],
        "fk": [("user_referral_id", "user_referrals", "referral_id", False)],
        "ts": "created_at",
    },
    "referral_rewards": {"pk": ["id"], "not_null": ["id", "reward_value", "created_at"], "fk": [], "ts": "created_at"},
    "user_referral_statuses": {"pk": ["id"], "not_null": ["id", "description"], "fk": [], "ts": "created_at"},
    "paid_transactions": {"pk": ["transaction_id"], "not_null": ["transaction_id", "transaction_status", "transaction_at"], "fk": [], "ts": "transaction_at"},
    "lead_log": {"pk": ["id"], "not_null": ["id", "lead_id", "created_at"], "fk": [], "ts": "created_at"},
}

default_args = {"owner": "you", "depends_on_past": False, "retries": 0}

def _load_cleaned() -> Dict[str, pd.DataFrame]:
    dfs = {}
    for tbl, fname in FILES.items():
        path = os.path.join(CLEAN_INPUT_DIR, fname)
        if not os.path.exists(path):
            raise AirflowException(f"Missing file: {path}")
        dfs[tbl] = pd.read_csv(path)
    return dfs

def _validate_task(**_):
    os.makedirs(REPORT_DIR, exist_ok=True)
    dfs = _load_cleaned()
    results: List[dict] = []

    def add_result(table, check, ok, details=""):
        results.append({"table": table, "check": check, "ok": bool(ok), "details": details})

    for table, rule in RULES.items():
        df = dfs[table]
        add_result(table, "row_count>0", len(df) > 0, f"rows={len(df)}")
        for col in rule["not_null"]:
            ok = df[col].notna().all() if col in df.columns else False
            details = f"nulls={df[col].isna().sum()}" if col in df.columns else "missing col"
            add_result(table, f"not_null:{col}", ok, details)
        pk = rule["pk"]
        if any(c not in df.columns for c in pk):
            missing = [c for c in pk if c not in df.columns]
            add_result(table, f"pk:{'+'.join(pk)}", False, f"missing cols {missing}")
        else:
            dup = df.duplicated(subset=pk).sum()
            add_result(table, f"pk_unique:{'+'.join(pk)}", dup == 0, f"duplicates={dup}")

    for table, rule in RULES.items():
        df = dfs[table]
        for (fk_col, ref_table, ref_col, nullable) in rule["fk"]:
            if fk_col not in df.columns:
                add_result(table, f"fk:{fk_col}->{ref_table}.{ref_col}", False, "fk column missing")
                continue
            ref_df = dfs[ref_table]
            if ref_col not in ref_df.columns:
                add_result(table, f"fk:{fk_col}->{ref_table}.{ref_col}", False, "ref column missing")
                continue
            fk_series = df[fk_col].dropna() if nullable else df[fk_col]
            missing = (~fk_series.isin(ref_df[ref_col])).sum()
            add_result(table, f"fk:{fk_col}->{ref_table}.{ref_col}", int(missing) == 0, f"violations={int(missing)}")

    with open(REPORT_JSON, "w") as f:
        json.dump(results, f, indent=2)
    pd.DataFrame(results).to_csv(REPORT_CSV, index=False)

    failed = [r for r in results if not r["ok"]]
    if failed:
        raise AirflowException(
            f"Validation failed for {len(failed)} checks. See {REPORT_JSON} / {REPORT_CSV}"
        )

def _missing_config_msg():
    need = []
    if not HOST_DAGS_DIR:
        need.append("HOST_DAGS_DIR (absolute host path to your dags folder)")
    if not HOST_CLEAN_DIR:
        need.append("HOST_CLEAN_DIR (absolute host path to your clean data folder)")
    msg = "Missing required env vars:\n - " + "\n - ".join(need) + \
          "\n\nAdd them to your .env and docker-compose so the Airflow webserver/scheduler inherit them."
    raise AirflowException(msg)

with DAG(
    dag_id="output_validation_and_publish_dag",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["subtask4", "iceberg", "publish"],
) as dag:

    # Always visible
    validate_outputs = PythonOperator(
        task_id="validate_outputs",
        python_callable=_validate_task,
    )

    if not HOST_DAGS_DIR or not HOST_CLEAN_DIR:
        # Keep DAG importable; show a clear, runnable task
        missing_config = PythonOperator(
            task_id="missing_config",
            python_callable=_missing_config_msg,
        )
        validate_outputs >> missing_config
    else:
        iceberg_ver = os.getenv("ICEBERG_SPARK_VERSION", "1.5.2")
        hadoop_ver = os.getenv("HADOOP_AZURE_VERSION", "3.3.6")
        azure_storage_ver = os.getenv("AZURE_STORAGE_JAVA_VERSION", "8.6.6")

        pkgs = ",".join([
            f"org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:{iceberg_ver}",
            f"org.apache.hadoop:hadoop-azure:{hadoop_ver}",
            f"org.apache.hadoop:hadoop-common:{hadoop_ver}",
            f"org.apache.hadoop:hadoop-auth:{hadoop_ver}",
            f"com.microsoft.azure:azure-storage:{azure_storage_ver}",
        ])

        account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
        account_key = os.getenv("AZURE_ACCOUNT_KEY", "")
        warehouse = os.getenv("ICEBERG_WAREHOUSE", "")

        publish_to_iceberg = DockerOperator(
            task_id="publish_to_iceberg",
            image="bitnami/spark:3.4",
            api_version="auto",
            auto_remove=True,
            docker_url="unix://var/run/docker.sock",
            command=(
                "spark-submit "
                f"--packages {pkgs} "
                "--conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog "
                "--conf spark.sql.catalog.lake.type=hadoop "
                f"--conf spark.sql.catalog.lake.warehouse={warehouse} "
                f"--conf spark.hadoop.fs.azure.account.key.{account}.dfs.core.windows.net={account_key} "
                f"--conf spark.hadoop.fs.azure.account.auth.type.{account}.dfs.core.windows.net=SharedKey "
                "--conf spark.hadoop.fs.AbstractFileSystem.abfs.impl=org.apache.hadoop.fs.azurebfs.Abfs "
                "--conf spark.hadoop.fs.AbstractFileSystem.abfss.impl=org.apache.hadoop.fs.azurebfs.Abfs "
                "--conf spark.sql.shuffle.partitions=1 "
                "/opt/airflow/dags/spark_job_publish_iceberg.py"

            
            ),
            environment={
                "AZURE_STORAGE_ACCOUNT": account,
                "AZURE_ACCOUNT_KEY": account_key,
                "ICEBERG_WAREHOUSE": warehouse,
                "ICEBERG_CATALOG": os.getenv("ICEBERG_CATALOG", ICEBERG_CATALOG),
                "PIPELINE_MODE": PIPELINE_MODE,
                "CLEAN_INPUT_DIR": CLEAN_INPUT_DIR,
            },
            network_mode="bridge",
            mounts=[
                Mount(source=HOST_DAGS_DIR,  target="/opt/airflow/dags",     type="bind"),
                Mount(source=HOST_CLEAN_DIR, target=CLEAN_INPUT_DIR,         type="bind"),
            ],
            mount_tmp_dir=False,
        )

        validate_outputs >> publish_to_iceberg
