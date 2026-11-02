"""Measures what the lakehouse layout buys you. Median of N runs, DuckDB, same machine.

    python -m pipeline.benchmark
"""
from __future__ import annotations

import json
import statistics
import time
from pathlib import Path

import duckdb

from . import config

RUNS = 5


def _size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _median_ms(sql: str) -> float:
    con = duckdb.connect()
    con.execute("PRAGMA threads=2")
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    return round(statistics.median(times), 1)


def main():
    days = sorted(p.name.split("=")[1] for p in config.RAW.glob("ingest_date=*"))
    d1, d7 = days[0], days[6]
    raw_glob = f"{config.RAW}/ingest_date=*/events.jsonl"
    cols = "{event_id:'VARCHAR',event_type:'VARCHAR',event_ts:'VARCHAR',user_id:'VARCHAR',product_id:'VARCHAR',price:'DOUBLE',quantity:'DOUBLE',order_id:'VARCHAR',device:'VARCHAR',country:'VARCHAR'}"
    sv = f"{config.SILVER}/**/*.parquet"

    # Q: purchases + revenue per day for the first 7 event days
    q_json = f"""SELECT CAST(try_cast(event_ts AS TIMESTAMP) AS DATE) d, COUNT(*), SUM(price*quantity)
                 FROM read_json('{config.RAW}/ingest_date=2026-09-0[1-7]/events.jsonl', format='newline_delimited', columns={cols})
                 WHERE event_type='purchase' GROUP BY 1"""
    q_pq_pruned = f"""SELECT event_date, COUNT(*), SUM(price*quantity)
                 FROM read_parquet('{sv}', hive_partitioning=true)
                 WHERE event_type='purchase' AND event_date BETWEEN DATE '{d1}' AND DATE '{d7}' GROUP BY 1"""
    # 1-day query: partition pruning (filter on the partition column) vs filter on a non-partition expression
    q_day_pruned = f"""SELECT COUNT(*), SUM(price*quantity) FROM read_parquet('{sv}', hive_partitioning=true)
                 WHERE event_type='purchase' AND event_date = DATE '{d1}'"""
    q_day_scan = f"""SELECT COUNT(*), SUM(price*quantity) FROM read_parquet('{sv}', hive_partitioning=false)
                 WHERE event_type='purchase' AND CAST(event_ts AS DATE) = DATE '{d1}'"""

    res = {
        "raw_json_bytes": _size(config.RAW),
        "bronze_parquet_bytes": _size(config.BRONZE),
        "silver_parquet_bytes": _size(config.SILVER),
        "gold_bytes": _size(config.GOLD),
        "query_7day_revenue_raw_json_ms": _median_ms(q_json),
        "query_7day_revenue_silver_parquet_ms": _median_ms(q_pq_pruned),
        "query_1day_partition_pruned_ms": _median_ms(q_day_pruned),
        "query_1day_no_pruning_ms": _median_ms(q_day_scan),
        "runs_per_query": RUNS,
    }
    res["json_to_bronze_compression_x"] = round(res["raw_json_bytes"] / res["bronze_parquet_bytes"], 1)
    res["json_vs_parquet_query_speedup_x"] = round(res["query_7day_revenue_raw_json_ms"] / res["query_7day_revenue_silver_parquet_ms"], 1)
    res["pruning_speedup_x"] = round(res["query_1day_no_pruning_ms"] / res["query_1day_partition_pruned_ms"], 1)
    config.METRICS.mkdir(parents=True, exist_ok=True)
    (config.METRICS / "benchmark.json").write_text(json.dumps(res, indent=2))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
