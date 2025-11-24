import os
from pyspark.sql import SparkSession, functions as F

AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_ACCOUNT_KEY = os.getenv("AZURE_ACCOUNT_KEY")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "lake")
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "overwrite").lower()  # overwrite | append
CLEAN_INPUT_DIR = os.getenv("CLEAN_INPUT_DIR", "/opt/airflow/data/staging/clean")

TABLES = {
    "user_referrals":        {"file": "user_referrals.clean.csv",        "ts": "referral_at"},
    "user_logs":             {"file": "user_logs.clean.csv",             "ts": None},
    "user_referral_logs":    {"file": "user_referral_logs.clean.csv",    "ts": "created_at"},
    "referral_rewards":      {"file": "referral_rewards.clean.csv",      "ts": "created_at"},
    "user_referral_statuses":{"file": "user_referral_statuses.clean.csv","ts": "created_at"},
    "paid_transactions":     {"file": "paid_transactions.clean.csv",     "ts": "transaction_at"},
    "lead_log":              {"file": "lead_log.clean.csv",              "ts": "created_at"},
}

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("publish_iceberg_adls")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # ADLS auth
        .config(f"spark.hadoop.fs.azure.account.key.{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net", AZURE_ACCOUNT_KEY)
        .getOrCreate()
    )
    return spark

def add_ymd(df, ts_col):
    """Add year/month/day columns when a timestamp column exists."""
    if not ts_col or ts_col not in df.columns:
        return df, []
    df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))
    df = (
        df.withColumn("year",  F.year(F.col(ts_col)))
          .withColumn("month", F.month(F.col(ts_col)))
          .withColumn("day",   F.dayofmonth(F.col(ts_col)))
    )
    return df, ["year", "month", "day"]

def ensure_table(spark: SparkSession, full_name: str, df, partitions):
    """Create table if missing, using DF schema (and partition spec when provided)."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.prod")
    view = f"schema_{full_name}"
    df.limit(0).createOrReplaceTempView(view)

    if partitions:
        part_spec = ", ".join(partitions)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.prod.{full_name}
            USING iceberg
            PARTITIONED BY ({part_spec})
            AS SELECT * FROM {view}
        """)
    else:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.prod.{full_name}
            USING iceberg
            AS SELECT * FROM {view}
        """)

def write_iceberg(spark: SparkSession, df, full_name: str, partitions, mode: str):
    ensure_table(spark, full_name, df, partitions)

    view = f"load_{full_name}"
    df.createOrReplaceTempView(view)

    if mode == "overwrite":
        # Truncate then load fresh
        spark.sql(f"DELETE FROM {ICEBERG_CATALOG}.prod.{full_name}")
        spark.sql(f"INSERT INTO {ICEBERG_CATALOG}.prod.{full_name} SELECT * FROM {view}")
    elif mode == "append":
        spark.sql(f"INSERT INTO {ICEBERG_CATALOG}.prod.{full_name} SELECT * FROM {view}")
    else:
        # default to overwrite if an unknown mode sneaks in
        spark.sql(f"DELETE FROM {ICEBERG_CATALOG}.prod.{full_name}")
        spark.sql(f"INSERT INTO {ICEBERG_CATALOG}.prod.{full_name} SELECT * FROM {view}")

def main():
    spark = build_spark()
    for tbl, meta in TABLES.items():
        path = os.path.join(CLEAN_INPUT_DIR, meta["file"])
        df = (
            spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(path)
        )
        df2, parts = add_ymd(df, meta["ts"])
        write_iceberg(spark, df2, tbl, parts, PIPELINE_MODE)
        print(f"Published {tbl} to {ICEBERG_CATALOG}.prod.{tbl}")
    spark.stop()

if __name__ == "__main__":
    main()
