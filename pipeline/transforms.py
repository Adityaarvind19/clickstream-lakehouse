"""Pure DataFrame -> DataFrame transformations (no I/O) so they are unit-testable."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import config

RAW_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts", T.StringType()),   # kept as string in bronze: never lose raw values
        T.StructField("user_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("price", T.DoubleType()),
        T.StructField("quantity", T.DoubleType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("device", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)

DQ_REASONS = [
    "CORRUPT_RECORD", "NULL_EVENT_ID", "NULL_USER", "BAD_TIMESTAMP",
    "INVALID_EVENT_TYPE", "NEGATIVE_PRICE", "UNKNOWN_PRODUCT",
]


def validate(events: DataFrame, products: DataFrame) -> DataFrame:
    """Tag each row with the FIRST rule it violates (null dq_reason == valid)."""
    known = products.select("product_id").distinct().withColumn("_known_product", F.lit(True))
    df = events.join(F.broadcast(known), "product_id", "left")
    ts = F.to_timestamp("event_ts")
    reason = (
        F.when(F.col("_corrupt_record").isNotNull(), "CORRUPT_RECORD")
        .when(F.col("event_id").isNull(), "NULL_EVENT_ID")
        .when(F.col("user_id").isNull(), "NULL_USER")
        .when(ts.isNull(), "BAD_TIMESTAMP")
        .when(F.col("event_type").isNull() | ~F.col("event_type").isin(*config.VALID_EVENT_TYPES), "INVALID_EVENT_TYPE")
        .when(F.col("price") < 0, "NEGATIVE_PRICE")
        .when(F.col("_known_product").isNull(), "UNKNOWN_PRODUCT")
    )
    return df.withColumn("dq_reason", reason).drop("_known_product")


def to_typed(valid: DataFrame) -> DataFrame:
    """Cast a validated bronze frame to the silver column types."""
    return (
        valid.withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .select(
            "event_id", "event_type", "event_ts", "event_date", "user_id", "product_id",
            "price", "quantity", "order_id", "device", "country", "ingest_date",
        )
    )


def dedupe(df: DataFrame) -> DataFrame:
    """Exactly-once by event_id; keep the earliest delivery (stable tie-break on event_ts)."""
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_date").asc(), F.col("event_ts").asc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def sessionize(df: DataFrame, gap_seconds: int = config.SESSION_GAP_SECONDS) -> DataFrame:
    """Gap-based sessions per (user, event_date). Sessions are cut at UTC midnight."""
    w = Window.partitionBy("user_id", "event_date").orderBy("event_ts", "event_id")
    prev = F.lag("event_ts").over(w)
    new_session = F.when(
        prev.isNull() | ((F.unix_timestamp("event_ts") - F.unix_timestamp(prev)) > gap_seconds), 1
    ).otherwise(0)
    w_cum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        df.withColumn("session_seq", F.sum(new_session).over(w_cum).cast("int"))
        .withColumn(
            "session_id",
            F.substring(F.sha2(F.concat_ws("|", "user_id", F.col("event_date").cast("string"), "session_seq"), 256), 1, 16),
        )
    )


def dim_user_scd2(history: DataFrame, users: DataFrame) -> DataFrame:
    """Type-2 slowly changing dimension on plan_tier from an effective-dated history."""
    h = history.withColumn("valid_from", F.to_date("effective_date"))
    w = Window.partitionBy("user_id").orderBy("valid_from")
    nxt = F.lead("valid_from").over(w)
    out = (
        h.withColumn("valid_to", F.coalesce(F.date_sub(nxt, 1), F.to_date(F.lit(config.OPEN_ENDED))))
        .withColumn("is_current", nxt.isNull())
        .join(users.select("user_id", "country", F.to_date("signup_date").alias("signup_date")), "user_id", "left")
    )
    return out.withColumn(
        "user_sk", F.substring(F.sha2(F.concat_ws("|", "user_id", F.col("valid_from").cast("string")), 256), 1, 16)
    ).select("user_sk", "user_id", "plan_tier", "country", "signup_date", "valid_from", "valid_to", "is_current")


def point_in_time_join(events: DataFrame, dim_user: DataFrame) -> DataFrame:
    """Attach the user's plan_tier AS OF the event date (not today's value)."""
    d = dim_user.select("user_sk", "user_id", "plan_tier", "valid_from", "valid_to")
    cond = (
        (events["user_id"] == d["user_id"])
        & (events["event_date"] >= d["valid_from"])
        & (events["event_date"] <= d["valid_to"])
    )
    return events.join(F.broadcast(d), cond, "left").drop(d["user_id"])


def dim_date(start: str, end: str, spark) -> DataFrame:
    days = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        (F.dayofweek("date").isin(1, 7)).alias("is_weekend"),
        F.weekofyear("date").alias("iso_week"),
        F.month("date").alias("month"),
        F.year("date").alias("year"),
    )
