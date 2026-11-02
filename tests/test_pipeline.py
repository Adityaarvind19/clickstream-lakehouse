import json
import subprocess
import sys
from datetime import date

import pytest
from pyspark.sql import Row

from pipeline import config
from pipeline.transforms import dedupe, dim_user_scd2, point_in_time_join, sessionize, to_typed, validate


@pytest.fixture(scope="session")
def spark():
    s = config.get_spark("tests", shuffle_partitions=4)
    yield s
    s.stop()


def ev(spark, rows):
    base = dict(event_id="e", event_type="page_view", event_ts="2026-09-01T10:00:00Z", user_id="U1",
                product_id="P1", price=10.0, quantity=None, order_id=None, device="web", country="IN",
                _corrupt_record=None, ingest_date=date(2026, 9, 1))
    return spark.createDataFrame([Row(**{**base, **r}) for r in rows], schema=None) if False else spark.createDataFrame(
        [{**base, **r} for r in rows],
        "event_id string, event_type string, event_ts string, user_id string, product_id string, price double, "
        "quantity double, order_id string, device string, country string, _corrupt_record string, ingest_date date",
    )


def products(spark):
    return spark.createDataFrame([("P1", "books", 10.0)], "product_id string, category string, base_price double")


def test_validate_assigns_first_failing_rule(spark):
    df = ev(spark, [
        dict(event_id="ok"),
        dict(event_id="nouser", user_id=None),
        dict(event_id="badts", event_ts="not-a-timestamp"),
        dict(event_id="badtype", event_type="hover"),
        dict(event_id="neg", price=-5.0),
        dict(event_id="noprod", product_id="P404"),
        dict(event_id="nouser_and_neg", user_id=None, price=-1.0),   # first rule wins -> NULL_USER
    ])
    got = {r["event_id"]: r["dq_reason"] for r in validate(df, products(spark)).collect()}
    assert got == {"ok": None, "nouser": "NULL_USER", "badts": "BAD_TIMESTAMP", "badtype": "INVALID_EVENT_TYPE",
                   "neg": "NEGATIVE_PRICE", "noprod": "UNKNOWN_PRODUCT", "nouser_and_neg": "NULL_USER"}


def test_dedupe_keeps_earliest_delivery(spark):
    df = ev(spark, [
        dict(event_id="a", ingest_date=date(2026, 9, 2)),
        dict(event_id="a", ingest_date=date(2026, 9, 1)),
        dict(event_id="b"),
    ])
    out = {(r["event_id"], r["ingest_date"]) for r in dedupe(to_typed(df)).collect()}
    assert out == {("a", date(2026, 9, 1)), ("b", date(2026, 9, 1))}


def test_sessionize_splits_on_gap_boundary(spark):
    # 1800s gap exactly -> same session; 1801s -> new session
    df = ev(spark, [
        dict(event_id="1", event_ts="2026-09-01T10:00:00Z"),
        dict(event_id="2", event_ts="2026-09-01T10:30:00Z"),   # +1800s  -> same
        dict(event_id="3", event_ts="2026-09-01T11:00:01Z"),   # +1801s  -> new
        dict(event_id="4", event_ts="2026-09-01T11:00:10Z"),   # same as 3
        dict(event_id="5", event_ts="2026-09-01T10:15:00Z", user_id="U2"),
    ])
    rows = {r["event_id"]: r["session_id"] for r in sessionize(to_typed(df)).collect()}
    assert rows["1"] == rows["2"]
    assert rows["2"] != rows["3"]
    assert rows["3"] == rows["4"]
    assert rows["5"] not in {rows["1"], rows["3"]}


def test_scd2_validity_ranges_and_point_in_time_join(spark):
    hist = spark.createDataFrame(
        [("U1", "free", "2026-01-01"), ("U1", "prime", "2026-09-10")], "user_id string, plan_tier string, effective_date string")
    users = spark.createDataFrame([("U1", "IN", "2026-01-01")], "user_id string, country string, signup_date string")
    d = dim_user_scd2(hist, users)
    rows = {r["plan_tier"]: r for r in d.collect()}
    assert rows["free"]["valid_to"] == date(2026, 9, 9) and not rows["free"]["is_current"]
    assert rows["prime"]["valid_to"] == date(9999, 12, 31) and rows["prime"]["is_current"]

    events = to_typed(ev(spark, [
        dict(event_id="before", event_ts="2026-09-09T12:00:00Z"),
        dict(event_id="after", event_ts="2026-09-10T00:00:01Z"),
    ]))
    got = {r["event_id"]: r["plan_tier"] for r in point_in_time_join(events, d).collect()}
    assert got == {"before": "free", "after": "prime"}


def _run(args, env_root):
    env = {**__import__("os").environ, "LAKE_ROOT": str(env_root)}
    return subprocess.run([sys.executable, *args], env=env, capture_output=True, text=True)


def test_end_to_end_is_idempotent_and_reconciles(tmp_path):
    """Generate a tiny dataset, run everything twice: same outputs, DQ gate green, nothing lost."""
    g = _run(["-m", "generator.generate", "--events", "30000", "--days", "3", "--users", "1500",
              "--products", "200", "--root", str(tmp_path)], tmp_path)
    assert g.returncode == 0, g.stderr

    def run_all():
        r = _run(["-m", "pipeline.run", "all"], tmp_path)
        assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-2000:]
        import duckdb
        con = duckdb.connect()
        s = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT event_id), SUM(hash(event_id)) "
                        f"FROM read_parquet('{tmp_path}/lake/silver/events/**/*.parquet')").fetchone()
        o = con.execute(f"SELECT COUNT(*), ROUND(SUM(net_amount),2) FROM read_parquet('{tmp_path}/lake/gold/fact_orders/**/*.parquet')").fetchone()
        return s, o

    first = run_all()
    second = run_all()                      # re-run: must not duplicate or drop anything
    assert first == second
    assert first[0][0] == first[0][1]       # unique event_id in silver

    # incremental re-run of ONE middle ingest day (late + past-midnight rows touch neighbouring
    # event_dates) must leave silver exactly as the full backfill produced it
    for stage in ("bronze", "silver", "gold"):
        r = _run(["-m", "pipeline.run", stage, "--from", "2026-09-02", "--to", "2026-09-02"], tmp_path)
        assert r.returncode == 0, r.stderr[-2000:]
    import duckdb
    s = duckdb.connect().execute(
        f"SELECT COUNT(*), COUNT(DISTINCT event_id), SUM(hash(event_id)) "
        f"FROM read_parquet('{tmp_path}/lake/silver/events/**/*.parquet')").fetchone()
    assert s == first[0]

    dq = json.loads((tmp_path / "metrics" / "dq_report.json").read_text())
    assert dq["failed_errors"] == 0
