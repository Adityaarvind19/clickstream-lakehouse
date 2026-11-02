"""Data-quality gate. Runs after gold; severity=error checks fail the pipeline (non-zero exit)."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from . import config, metrics


def run(spark) -> dict:
    results = []

    def check(name, passed, severity="error", detail=None):
        results.append({"check": name, "passed": bool(passed), "severity": severity, "detail": detail})

    with metrics.timed("dq"):
        silver = spark.read.parquet(str(config.SILVER))
        fe = spark.read.parquet(str(config.GOLD / "fact_events"))
        fo = spark.read.parquet(str(config.GOLD / "fact_orders"))
        dp = spark.read.parquet(str(config.GOLD / "dim_product"))
        du = spark.read.parquet(str(config.GOLD / "dim_user"))
        daily = spark.read.parquet(str(config.GOLD / "agg_daily_revenue"))
        m = metrics.load()

        n = silver.count()
        distinct = silver.select("event_id").distinct().count()
        check("silver.event_id is unique", n == distinct, detail={"rows": n, "distinct": distinct})

        nulls = silver.select(
            *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["event_id", "event_ts", "user_id", "product_id", "session_id"]]
        ).first().asDict()
        check("silver key columns are not null", all(v == 0 for v in nulls.values()), detail=nulls)

        bad_price = silver.filter(F.col("price") < 0).count()
        bad_qty = silver.filter(F.col("event_type").isin("purchase", "add_to_cart") & (F.col("quantity") < 1)).count()
        check("price >= 0 and quantity >= 1 on cart/purchase", bad_price == 0 and bad_qty == 0, detail={"bad_price": bad_price, "bad_qty": bad_qty})

        orphan_p = fe.join(dp.select("product_id"), "product_id", "left_anti").count()
        check("RI: fact_events.product_id in dim_product", orphan_p == 0, detail={"orphans": orphan_p})
        no_user = fe.filter(F.col("user_sk").isNull()).count()
        check("RI: fact_events resolves to a dim_user row (point-in-time)", no_user == 0, detail={"unresolved": no_user})

        overlap = (
            du.alias("a").join(du.alias("b"), (F.col("a.user_id") == F.col("b.user_id")) & (F.col("a.valid_from") < F.col("b.valid_from")) & (F.col("a.valid_to") >= F.col("b.valid_from")))
            .count()
        )
        cur = du.filter("is_current").groupBy("user_id").count().filter("count != 1").count()
        check("dim_user SCD2: no overlapping validity ranges, exactly one current row/user", overlap == 0 and cur == 0, detail={"overlaps": overlap, "bad_current": cur})

        p_silver = silver.filter("event_type = 'purchase'").count()
        check("fact_orders rows == silver purchases", fo.count() == p_silver, detail={"fact_orders": fo.count(), "silver_purchases": p_silver})

        g1 = fo.agg(F.round(F.sum("gross_amount"), 2)).first()[0]
        g2 = daily.agg(F.round(F.sum("gross_revenue"), 2)).first()[0]
        check("mart reconciles to fact: sum(gross_revenue)", abs((g1 or 0) - (g2 or 0)) < 1.0, detail={"fact": g1, "mart": g2})

        w = Window.partitionBy("session_id").orderBy("event_ts")
        gap = (
            fe.withColumn("_gap", F.unix_timestamp("event_ts") - F.unix_timestamp(F.lag("event_ts").over(w)))
            .agg(F.max("_gap")).first()[0]
        )
        check("sessionization: max intra-session gap <= 30 min", (gap or 0) <= config.SESSION_GAP_SECONDS, detail={"max_gap_seconds": gap})

        s = m.get("silver", {})
        check("reconciliation: ingest rows == quarantined + valid", s.get("recon_window_ok", False), detail={k: s.get(k) for k in ["rows_in", "quarantined", "valid_in_window"]})
        check("reconciliation: silver rows == distinct valid event_ids", s.get("recon_silver_ok", False), detail={k: s.get(k) for k in ["recomputed_rows", "duplicates_removed", "silver_rows"]})

        # edge dates (before the first / after the last ingest day) only hold late stragglers -> skip them
        ing_lo, ing_hi = silver.agg(F.min("ingest_date"), F.max("ingest_date")).first()
        counts = [
            r["n"]
            for r in silver.filter((F.col("event_date") >= ing_lo) & (F.col("event_date") <= ing_hi))
            .groupBy("event_date").agg(F.count("*").alias("n")).collect()
        ]
        counts.sort()
        med = counts[len(counts) // 2]
        outliers = [c for c in counts if c < 0.5 * med or c > 1.5 * med]
        check("volume: daily rows within +-50% of median", not outliers, severity="warn", detail={"median": med, "outliers": len(outliers)})

        orphan_refunds = fe.filter("event_type='refund'").join(fo.select("order_id"), "order_id", "left_anti").count()
        check("refunds reference a known purchase", orphan_refunds == 0, severity="warn", detail={"orphan_refunds": orphan_refunds})

    errors = [r for r in results if not r["passed"] and r["severity"] == "error"]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_checks": len(results),
        "passed": sum(r["passed"] for r in results),
        "failed_errors": len(errors),
        "results": results,
    }
    config.METRICS.mkdir(parents=True, exist_ok=True)
    (config.METRICS / "dq_report.json").write_text(json.dumps(report, indent=2, default=str))
    metrics.record("dq", total_checks=len(results), passed=report["passed"], failed_errors=len(errors))
    for r in results:
        print(("PASS" if r["passed"] else ("FAIL" if r["severity"] == "error" else "WARN")), "-", r["check"])
    if errors:
        raise SystemExit(f"{len(errors)} data-quality error(s)")
    return report
