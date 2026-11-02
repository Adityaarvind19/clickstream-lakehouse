"""Synthetic e-commerce clickstream generator.

Produces session-structured events (page_view -> add_to_cart -> purchase -> refund)
with the data problems a real pipeline must survive:
  * duplicate events (within a file and re-delivered the next day)
  * late-arriving events (event_ts 1-2 days before the file's ingest_date)
  * malformed records (null user, negative price, bad timestamp, unknown event
    type, unknown product)
  * out-of-order rows and heavy-tailed user / product popularity

Everything is SYNTHETIC and seeded, so runs are reproducible.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline import config

HOUR_P = np.array([1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 6, 7, 7, 6, 6, 6, 7, 8, 9, 9, 8, 5, 3], dtype=float)
HOUR_P /= HOUR_P.sum()
COUNTRIES = np.array(["IN", "US", "GB", "DE", "AE", "SG", "CA", "AU"])
COUNTRY_P = np.array([0.38, 0.22, 0.09, 0.07, 0.07, 0.06, 0.06, 0.05])
CATEGORIES = np.array(["electronics", "books", "home", "fashion", "grocery", "toys", "beauty", "sports"])


def make_reference(rng, n_users: int, n_products: int, start: date, days: int, out: Path) -> np.ndarray:
    out.mkdir(parents=True, exist_ok=True)
    pid = np.array([f"P{i:05d}" for i in range(n_products)])
    base = np.round(np.exp(rng.normal(3.6, 0.9, n_products)).clip(1.5, 900), 2)
    pd.DataFrame(
        {"product_id": pid, "category": rng.choice(CATEGORIES, n_products), "base_price": base}
    ).to_csv(out / "products.csv", index=False)

    uid = np.array([f"U{i:06d}" for i in range(n_users)])
    # signup >= MAX_LATENESS_DAYS+1 before start, so no (late-shifted) event can predate its user
    signup = [start - timedelta(days=int(d)) for d in rng.integers(config.MAX_LATENESS_DAYS + 1, 900, n_users)]
    country = rng.choice(COUNTRIES, n_users, p=COUNTRY_P)
    pd.DataFrame({"user_id": uid, "country": country, "signup_date": signup}).to_csv(out / "users.csv", index=False)

    # plan history (feeds SCD Type 2): everyone starts on a tier; ~15% change once in the window
    tiers = np.array(["free", "plus", "prime"])
    first = rng.choice(tiers, n_users, p=[0.7, 0.2, 0.1])
    rows = [(u, t, s) for u, t, s in zip(uid, first, signup)]
    changers = rng.choice(n_users, int(n_users * 0.15), replace=False)
    for i in changers:
        new = tiers[(list(tiers).index(first[i]) + 1) % 3]
        rows.append((uid[i], new, start + timedelta(days=int(rng.integers(1, days)))))
    pd.DataFrame(rows, columns=["user_id", "plan_tier", "effective_date"]).to_csv(out / "user_plan_history.csv", index=False)
    return base


def gen_day(rng, day: date, n_target: int, n_users: int, n_products: int, base_price, user_country_idx):
    lens = np.minimum(rng.geometric(0.22, int(n_target / 4.5)), 40)
    n_sess, total = len(lens), int(lens.sum())
    sess_user = (rng.random(n_sess) ** 2.2 * n_users).astype(np.int64)            # heavy-tailed users
    sess_start = rng.choice(24, n_sess, p=HOUR_P) * 3600 + rng.integers(0, 3600, n_sess)
    idx = np.repeat(np.arange(n_sess), lens)
    starts = np.cumsum(lens) - lens
    gaps = rng.exponential(45.0, total)
    gaps[starts] = 0
    cs = np.cumsum(gaps)
    ts_sec = (sess_start[idx] + (cs - np.repeat(cs[starts], lens))).astype(np.int64)

    etype = rng.choice(config.VALID_EVENT_TYPES, total, p=[0.78, 0.14, 0.077, 0.003])
    etype[starts] = "page_view"
    user_idx = sess_user[idx]
    prod_idx = (rng.random(total) ** 1.8 * n_products).astype(np.int64)
    price = np.round(base_price[prod_idx] * rng.choice([1.0, 0.9, 0.8], total, p=[0.8, 0.15, 0.05]), 2)

    qty = 1 + (rng.random(total) < 0.2).astype(int) + (rng.random(total) < 0.05).astype(int)
    qty = np.where(etype == "page_view", 0, qty)

    day_tag = f"{day:%y%m%d}"
    order_id = np.full(total, None, dtype=object)
    purchases = np.where(etype == "purchase")[0]
    order_id[purchases] = [f"O{day_tag}{i:06d}" for i in range(len(purchases))]
    refunds = np.where(etype == "refund")[0]
    if len(purchases) and len(refunds):
        src = rng.choice(purchases, len(refunds))
        order_id[refunds] = order_id[src]
        prod_idx[refunds], price[refunds], qty[refunds] = prod_idx[src], price[src], qty[src]

    late = rng.random(total) < 0.02                                                 # late arrivals
    ts_sec = ts_sec - np.where(late, rng.choice([86400, 172800], total), 0)

    base = np.datetime64(day.isoformat(), "s")
    ts = (base + ts_sec.astype("timedelta64[s]")).astype(str)
    df = pd.DataFrame(
        {
            "event_id": [f"{day_tag}-{i:07d}" for i in range(total)],
            "event_type": etype,
            "event_ts": np.char.add(ts.astype(str), "Z"),
            "user_id": [f"U{i:06d}" for i in user_idx],
            "product_id": [f"P{i:05d}" for i in prod_idx],
            "price": price,
            "quantity": np.where(qty == 0, np.nan, qty),
            "order_id": order_id,
            "device": rng.choice(["mobile", "web", "tablet"], total, p=[0.55, 0.35, 0.10]),
            "country": COUNTRIES[user_country_idx[user_idx]],
        }
    )
    return df


def corrupt(rng, df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    pick = lambda frac: rng.random(n) < frac  # noqa: E731
    df.loc[pick(0.003), "user_id"] = None
    m = pick(0.003); df.loc[m, "price"] = -df.loc[m, "price"].abs() - 1
    df.loc[pick(0.002), "event_ts"] = "not-a-timestamp"
    df.loc[pick(0.002), "event_type"] = "unknown_evt"
    df.loc[pick(0.002), "product_id"] = "P99999"
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=10_000_000)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--users", type=int, default=100_000)
    ap.add_argument("--products", type=int, default=5_000)
    ap.add_argument("--start", default="2026-09-01")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--root", default=None, help="override LAKE_ROOT")
    a = ap.parse_args()

    raw = Path(a.root) / "raw" if a.root else config.RAW
    ref = Path(a.root) / "reference" if a.root else config.REF
    rng = np.random.default_rng(a.seed)
    start = date.fromisoformat(a.start)
    t0 = time.time()
    base = make_reference(rng, a.users, a.products, start, a.days, ref)
    user_country_idx = np.array(
        [list(COUNTRIES).index(c) for c in pd.read_csv(ref / "users.csv")["country"]]
    )

    per_day = a.events // a.days
    carry = None
    written = 0
    for d in range(a.days):
        day = start + timedelta(days=d)
        n = int(per_day * (1.15 if day.weekday() >= 5 else 0.95))
        df = gen_day(rng, day, n, a.users, a.products, base, user_country_idx)
        df = pd.concat([df, df.sample(frac=0.015, random_state=int(rng.integers(1 << 30)))])  # in-file dups
        if carry is not None:
            df = pd.concat([df, carry])                                                          # re-delivery
        carry = df.sample(frac=0.005, random_state=int(rng.integers(1 << 30)))
        df = corrupt(rng, df.sample(frac=1.0, random_state=int(rng.integers(1 << 30))).reset_index(drop=True))
        out = raw / f"ingest_date={day.isoformat()}"
        out.mkdir(parents=True, exist_ok=True)
        df.to_json(out / "events.jsonl", orient="records", lines=True)
        written += len(df)
        print(f"{day} rows={len(df):,}", flush=True)

    summary = {"raw_rows": written, "days": a.days, "seconds": round(time.time() - t0, 1)}
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
