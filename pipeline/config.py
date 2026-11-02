"""Central configuration: paths, constants, Spark session factory.

All storage paths are plain filesystem paths so the same code maps 1:1 to S3
(`s3a://bucket/...`) by setting LAKE_ROOT -- see README "AWS mapping".
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(os.environ.get("LAKE_ROOT", Path(__file__).resolve().parent.parent / "data"))

RAW = ROOT / "raw"                      # landing zone: JSONL per ingest_date
REF = ROOT / "reference"                # users / products / plan history (CSV)
BRONZE = ROOT / "lake" / "bronze" / "events"
SILVER = ROOT / "lake" / "silver" / "events"
QUARANTINE = ROOT / "lake" / "quarantine" / "events"
GOLD = ROOT / "lake" / "gold"
WAREHOUSE = ROOT / "warehouse" / "analytics.duckdb"
METRICS = ROOT / "metrics"
REPORTS = ROOT / "reports"

SESSION_GAP_SECONDS = 30 * 60           # new session after 30 min inactivity
MAX_LATENESS_DAYS = 2                   # late events arrive up to 2 days after event_date
MAX_EARLINESS_DAYS = 1                  # a file can hold events just past midnight (next event_date)
VALID_EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "refund"]
OPEN_ENDED = "9999-12-31"               # valid_to for current SCD2 rows


def get_spark(app_name: str = "clickstream-lakehouse", shuffle_partitions: int = 16):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # idempotent partition rewrites
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
