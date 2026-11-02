"""Serving layer: a DuckDB 'warehouse' of views over the gold Parquet (Athena/Redshift-Spectrum style).

    python -m pipeline.warehouse            # builds views, runs sql/analytics.sql, prints timings
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import duckdb

from . import config

TABLES = ["dim_user", "dim_product", "dim_date", "fact_events", "fact_orders",
          "agg_daily_revenue", "agg_funnel_daily", "agg_product_daily"]
SQL_FILE = Path(__file__).resolve().parent.parent / "sql" / "analytics.sql"


def connect(read_only: bool = False):
    config.WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(config.WAREHOUSE), read_only=read_only)
    if not read_only:
        for t in TABLES:
            glob = f"{config.GOLD / t}/**/*.parquet"
            con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{glob}', hive_partitioning = true)")
    return con


def queries():
    text = SQL_FILE.read_text()
    parts = re.split(r"^-- name: (\w+)\s*$", text, flags=re.M)
    return [(parts[i], parts[i + 1].strip()) for i in range(1, len(parts), 2)]


def main():
    con = connect()
    out = {}
    for name, sql in queries():
        t0 = time.time()
        rows = con.execute(sql).fetchall()
        out[name] = {"rows": len(rows), "seconds": round(time.time() - t0, 3)}
        print(f"{name:40s} {len(rows):>4} rows  {out[name]['seconds']}s")
    config.METRICS.mkdir(parents=True, exist_ok=True)
    (config.METRICS / "warehouse_queries.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
