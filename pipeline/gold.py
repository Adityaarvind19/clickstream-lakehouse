"""Gold: star schema (dims + facts) and pre-aggregated marts, rebuilt from silver each run."""
from __future__ import annotations

from pyspark.sql import functions as F

from . import config, metrics
from .transforms import dim_date, dim_user_scd2, point_in_time_join


def _w(df, name, part=None):
    path = str(config.GOLD / name)
    w = df.write.mode("overwrite")
    (w.partitionBy(part) if part else w).parquet(path)
    return path


def run(spark) -> dict:
    with metrics.timed("gold"):
        silver = spark.read.parquet(str(config.SILVER))
        products = spark.read.option("header", True).csv(str(config.REF / "products.csv")).select(
            "product_id", "category", F.col("base_price").cast("double").alias("base_price")
        )
        users = spark.read.option("header", True).csv(str(config.REF / "users.csv"))
        hist = spark.read.option("header", True).csv(str(config.REF / "user_plan_history.csv"))

        # ---- dimensions --------------------------------------------------------------
        d_user = dim_user_scd2(hist, users)
        lo, hi = silver.agg(F.min("event_date"), F.max("event_date")).first()
        d_date = dim_date(str(lo), str(hi), spark)
        _w(d_user, "dim_user")
        _w(products, "dim_product")
        _w(d_date, "dim_date")

        # ---- fact_events (point-in-time plan tier, clustered by event_date) ------------
        fe = (
            point_in_time_join(silver, spark.read.parquet(str(config.GOLD / "dim_user")))
            .join(products.select("product_id", "category"), "product_id", "left")
            .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
            .select(
                "event_id", "event_type", "event_ts", "date_key", "user_sk", "user_id", "plan_tier",
                "product_id", "category", "price", "quantity", "order_id", "device", "country",
                "session_id", "event_date",
            )
        )
        fe.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet(str(config.GOLD / "fact_events"))
        fact_events = spark.read.parquet(str(config.GOLD / "fact_events"))

        # ---- fact_orders: one row per purchase, refund netted (at most one refund honoured)
        purchases = fact_events.filter("event_type = 'purchase'").select(
            "order_id", "user_sk", "user_id", "product_id", "category", "plan_tier", "device",
            F.col("event_date").alias("order_date"), F.col("event_ts").alias("order_ts"),
            "quantity", "price", (F.col("price") * F.col("quantity")).alias("gross_amount"),
        )
        refunds = (
            fact_events.filter("event_type = 'refund'")
            .groupBy("order_id")
            .agg(F.max(F.col("price") * F.col("quantity")).alias("refund_amount"), F.min("event_ts").alias("refund_ts"))
        )
        fo = (
            purchases.join(refunds, "order_id", "left")
            .withColumn("refund_amount", F.coalesce("refund_amount", F.lit(0.0)))
            .withColumn("net_amount", F.col("gross_amount") - F.col("refund_amount"))
        )
        fo.repartition("order_date").write.mode("overwrite").partitionBy("order_date").parquet(str(config.GOLD / "fact_orders"))
        fact_orders = spark.read.parquet(str(config.GOLD / "fact_orders"))

        # ---- marts ---------------------------------------------------------------------
        daily = fact_orders.groupBy("order_date").agg(
            F.count("*").alias("orders"),
            F.sum("quantity").alias("units"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("refund_amount"), 2).alias("refunds"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            F.round(F.avg("gross_amount"), 2).alias("avg_order_value"),
        )
        _w(daily, "agg_daily_revenue")

        flags = fact_events.groupBy("event_date", "session_id").agg(
            F.max("plan_tier").alias("plan_tier"),
            F.max((F.col("event_type") == "page_view").cast("int")).alias("viewed"),
            F.max((F.col("event_type") == "add_to_cart").cast("int")).alias("carted"),
            F.max((F.col("event_type") == "purchase").cast("int")).alias("purchased"),
        )
        funnel = flags.groupBy("event_date", "plan_tier").agg(
            F.count("*").alias("sessions"),
            F.sum("viewed").alias("sessions_viewed"),
            F.sum("carted").alias("sessions_carted"),
            F.sum("purchased").alias("sessions_purchased"),
        ).withColumn("conversion_rate", F.round(F.col("sessions_purchased") / F.col("sessions"), 4))
        _w(funnel, "agg_funnel_daily")

        prod = fact_events.groupBy("event_date", "product_id", "category").agg(
            F.sum((F.col("event_type") == "page_view").cast("int")).alias("views"),
            F.sum((F.col("event_type") == "add_to_cart").cast("int")).alias("carts"),
            F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"),
            F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price") * F.col("quantity")).otherwise(0.0)), 2).alias("revenue"),
        )
        _w(prod, "agg_product_daily")

        stats = {
            "dim_user_rows": spark.read.parquet(str(config.GOLD / "dim_user")).count(),
            "fact_events_rows": fact_events.count(),
            "fact_orders_rows": fact_orders.count(),
            "agg_daily_revenue_rows": daily.count(),
        }
        metrics.record("gold", **stats)
        return stats
