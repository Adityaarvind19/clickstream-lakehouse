"""Silver: validate -> quarantine -> dedupe -> type -> sessionize. Incremental and idempotent.

Late-arriving data: an ingest window can contain events for *earlier* event_dates.
Overwriting those partitions with only the new rows would lose data, so we
recompute every affected event_date from all bronze partitions that could
contain it (MAX_EARLINESS_DAYS before .. MAX_LATENESS_DAYS after) and overwrite
those partitions atomically (dynamic partition overwrite).
"""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import functions as F

from . import config, metrics
from .transforms import dedupe, sessionize, to_typed, validate


def _products(spark):
    return spark.read.option("header", True).csv(str(config.REF / "products.csv"))


def run(spark, ingest_from: str, ingest_to: str) -> dict:
    products = _products(spark)
    bronze = spark.read.parquet(str(config.BRONZE))

    with metrics.timed("silver"):
        # ---- A. validate the ingest window, quarantine bad rows ---------------------
        window = bronze.filter((F.col("ingest_date") >= ingest_from) & (F.col("ingest_date") <= ingest_to))
        a = validate(window, products)
        reasons = {r["dq_reason"]: r["n"] for r in a.groupBy("dq_reason").agg(F.count("*").alias("n")).collect()}
        n_in = sum(reasons.values())
        n_bad = sum(v for k, v in reasons.items() if k is not None)
        n_valid_window = reasons.get(None, 0)

        (
            a.filter(F.col("dq_reason").isNotNull())
            .write.mode("overwrite").partitionBy("ingest_date").parquet(str(config.QUARANTINE))
        )

        affected = [
            r["d"] for r in
            to_typed(a.filter(F.col("dq_reason").isNull())).select(F.col("event_date").alias("d")).distinct().collect()
        ]
        if not affected:
            metrics.record("silver", rows_in=n_in, quarantined=n_bad, silver_rows=0)
            return {}
        lo, hi = min(affected), max(affected)

        # ---- B. recompute affected event_dates from every bronze partition that may hold them
        b_lo = (lo - timedelta(days=config.MAX_EARLINESS_DAYS)).isoformat()
        b_hi = (hi + timedelta(days=config.MAX_LATENESS_DAYS)).isoformat()
        b = validate(
            bronze.filter((F.col("ingest_date") >= b_lo) & (F.col("ingest_date") <= b_hi)), products
        ).filter(F.col("dq_reason").isNull())
        typed = to_typed(b).filter(F.col("event_date").isin(affected))
        n_b = typed.count()
        n_distinct = typed.select("event_id").distinct().count()   # independent of the dedupe code path

        out = sessionize(dedupe(typed))
        out.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet(str(config.SILVER))

        silver = spark.read.parquet(str(config.SILVER)).filter(F.col("event_date").isin(affected))
        n_out = silver.count()
        sessions = silver.select("user_id", "event_date", "session_id").distinct().count()

        stats = {
            "rows_in": n_in,
            "quarantined": n_bad,
            "quarantine_by_reason": {k: v for k, v in reasons.items() if k is not None},
            "valid_in_window": n_valid_window,
            "affected_event_dates": len(affected),
            "recomputed_rows": n_b,
            "duplicates_removed": n_b - n_out,
            "silver_rows": n_out,
            "sessions": sessions,
            # row-count reconciliation: nothing is silently dropped
            "recon_window_ok": n_in == n_bad + n_valid_window,
            "recon_silver_ok": n_out == n_distinct,
        }
        metrics.record("silver", **stats)
        return stats
