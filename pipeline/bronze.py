"""Bronze: land raw JSONL as immutable, schema-enforced Parquet partitioned by ingest_date."""
from __future__ import annotations

from pyspark.sql import functions as F

from . import config, metrics
from .transforms import RAW_SCHEMA


def _dirs(lo: str | None, hi: str | None):
    out = []
    for p in sorted(config.RAW.glob("ingest_date=*")):
        d = p.name.split("=")[1]
        if (lo is None or d >= lo) and (hi is None or d <= hi):
            out.append(str(p))
    return out


def run(spark, ingest_from: str | None = None, ingest_to: str | None = None) -> int:
    paths = _dirs(ingest_from, ingest_to)
    if not paths:
        raise SystemExit("no raw partitions found for the requested range")
    with metrics.timed("bronze"):
        df = (
            spark.read.schema(RAW_SCHEMA)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(paths)
            .withColumn("_src", F.col("_metadata.file_path"))
            .withColumn(
                "ingest_date",
                F.to_date(F.regexp_extract("_src", r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)),
            )
            .drop("_src")
        )
        # dynamic partition overwrite -> re-running a date replaces only that partition (idempotent)
        df.write.mode("overwrite").partitionBy("ingest_date").parquet(str(config.BRONZE))
    lo = min(p.split("=")[1] for p in paths)
    hi = max(p.split("=")[1] for p in paths)
    n = (
        spark.read.parquet(str(config.BRONZE))
        .filter((F.col("ingest_date") >= lo) & (F.col("ingest_date") <= hi))
        .count()
    )
    raw_bytes = sum(f.stat().st_size for p in paths for f in __import__("pathlib").Path(p).glob("*.jsonl"))
    metrics.record("bronze", rows=n, partitions=len(paths), raw_bytes=raw_bytes)
    return n
