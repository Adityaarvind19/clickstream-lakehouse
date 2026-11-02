"""Reporting layer: one self-contained HTML dashboard from the warehouse + run metrics.

    python -m pipeline.report   ->  data/reports/dashboard.html
"""
from __future__ import annotations

import base64
import io
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from . import config, warehouse  # noqa: E402


def _png(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def main():
    con = warehouse.connect()
    q = dict(warehouse.queries())
    rev = con.execute(q["rolling_7d_net_revenue"]).fetchdf()
    funnel = con.execute(q["funnel_by_plan_tier"]).fetchdf()
    refunds = con.execute(q["refund_rate_by_category"]).fetchdf()
    # drop edge dates that only hold late stragglers / past-midnight spill, then re-roll the average
    rev = rev[rev["net_revenue"] > 0.5 * rev["net_revenue"].median()].reset_index(drop=True)
    rev["net_revenue_7d_avg"] = rev["net_revenue"].rolling(7, min_periods=1).mean()

    figs = []
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.bar(rev["order_date"], rev["net_revenue"], color="#9db4d6", label="daily net revenue")
    ax.plot(rev["order_date"], rev["net_revenue_7d_avg"], color="#1f4e8c", lw=2, label="7-day average")
    ax.set_title("Net revenue"); ax.legend(frameon=False); ax.tick_params(axis="x", rotation=45)
    figs.append(("Net revenue with 7-day rolling average", _png(fig)))

    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.bar(funnel["plan_tier"], funnel["view_to_purchase"] * 100, color="#1f4e8c")
    ax.set_ylabel("sessions converting (%)"); ax.set_title("Session-to-purchase conversion by plan tier (point-in-time)")
    figs.append(("Funnel by plan tier", _png(fig)))

    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.barh(refunds["category"], refunds["refund_rate"] * 100, color="#c2603b")
    ax.set_xlabel("orders refunded (%)"); ax.set_title("Refund rate by category")
    figs.append(("Refund rate by category", _png(fig)))

    m = json.loads((config.METRICS / "latest.json").read_text()) if (config.METRICS / "latest.json").exists() else {}
    dqp = config.METRICS / "dq_report.json"
    dq = json.loads(dqp.read_text()) if dqp.exists() else {"results": []}

    rows = "".join(
        f"<tr><td>{'PASS' if r['passed'] else r['severity'].upper()}</td><td>{r['check']}</td></tr>" for r in dq["results"]
    )
    imgs = "".join(f"<h3>{t}</h3><img src='data:image/png;base64,{b}' style='max-width:100%'>" for t, b in figs)
    s = m.get("silver", {})
    html = f"""<!doctype html><meta charset=utf-8><title>Clickstream Lakehouse - Dashboard</title>
<style>body{{font:15px system-ui;max-width:860px;margin:2rem auto;padding:0 1rem;color:#1b2430}}
td{{padding:2px 12px 2px 0}}h1{{margin-bottom:0}}.k{{display:flex;gap:2rem;margin:1rem 0}}.k div b{{display:block;font-size:1.5rem}}</style>
<h1>Clickstream Lakehouse</h1><p>Synthetic e-commerce events &middot; bronze / silver / gold on Parquet</p>
<div class=k><div><b>{s.get('rows_in', 0):,}</b>raw rows</div><div><b>{s.get('quarantined', 0):,}</b>quarantined</div>
<div><b>{s.get('duplicates_removed', 0):,}</b>duplicates removed</div><div><b>{s.get('silver_rows', 0):,}</b>silver rows</div></div>
{imgs}<h3>Data-quality gate ({dq.get('passed', 0)}/{dq.get('total_checks', 0)} passed)</h3><table>{rows}</table>"""
    config.REPORTS.mkdir(parents=True, exist_ok=True)
    out = config.REPORTS / "dashboard.html"
    out.write_text(html)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
