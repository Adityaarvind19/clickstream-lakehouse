"""CLI orchestrator.

    python -m pipeline.run all    --from 2026-09-01 --to 2026-09-30
    python -m pipeline.run bronze|silver|gold|dq [--from D --to D]

Every stage is idempotent: re-running a date range overwrites only its partitions.
"""
from __future__ import annotations

import argparse
import json
import sys

from . import bronze, config, dq, gold, metrics, silver


def main(argv=None):
    ap = argparse.ArgumentParser(prog="pipeline.run")
    ap.add_argument("stage", choices=["bronze", "silver", "gold", "dq", "all"])
    ap.add_argument("--from", dest="d_from", default=None, help="first ingest_date (YYYY-MM-DD)")
    ap.add_argument("--to", dest="d_to", default=None, help="last ingest_date (YYYY-MM-DD)")
    a = ap.parse_args(argv)

    spark = config.get_spark()
    try:
        if a.stage in ("bronze", "all"):
            bronze.run(spark, a.d_from, a.d_to)
        if a.stage in ("silver", "all"):
            lo = a.d_from or min(p.name.split("=")[1] for p in config.BRONZE.glob("ingest_date=*"))
            hi = a.d_to or max(p.name.split("=")[1] for p in config.BRONZE.glob("ingest_date=*"))
            silver.run(spark, lo, hi)
        if a.stage in ("gold", "all"):
            gold.run(spark)
        if a.stage in ("dq", "all"):
            dq.run(spark)
    finally:
        spark.stop()
    print(json.dumps(metrics.load(), indent=2))


if __name__ == "__main__":
    sys.exit(main())
