-- Analytics queries over the gold star schema (run by `python -m pipeline.warehouse`).
-- Each block is: -- name: <id>   followed by one statement.

-- name: rolling_7d_net_revenue
SELECT order_date,
       net_revenue,
       ROUND(AVG(net_revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS net_revenue_7d_avg,
       ROUND(net_revenue / NULLIF(LAG(net_revenue, 7) OVER (ORDER BY order_date), 0) - 1, 4)       AS wow_growth
FROM agg_daily_revenue
ORDER BY order_date;

-- name: top3_products_per_category
WITH ranked AS (
  SELECT category, product_id, SUM(revenue) AS revenue,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(revenue) DESC) AS rk
  FROM agg_product_daily
  GROUP BY category, product_id
)
SELECT category, product_id, ROUND(revenue, 2) AS revenue, rk
FROM ranked WHERE rk <= 3 ORDER BY category, rk;

-- name: funnel_by_plan_tier
SELECT plan_tier,
       SUM(sessions)           AS sessions,
       SUM(sessions_carted)    AS carted,
       SUM(sessions_purchased) AS purchased,
       ROUND(SUM(sessions_carted)    * 1.0 / SUM(sessions), 4) AS view_to_cart,
       ROUND(SUM(sessions_purchased) * 1.0 / SUM(sessions), 4) AS view_to_purchase
FROM agg_funnel_daily
GROUP BY plan_tier ORDER BY view_to_purchase DESC;

-- name: plan_tier_point_in_time_vs_current
-- Shows why SCD2 matters: revenue attributed to the tier a user HAD at order time vs the tier they have today.
SELECT o.plan_tier AS tier_at_order, u.plan_tier AS tier_today,
       COUNT(*) AS orders, ROUND(SUM(o.net_amount), 2) AS net_revenue
FROM fact_orders o
JOIN dim_user u ON u.user_id = o.user_id AND u.is_current
GROUP BY 1, 2 ORDER BY orders DESC;

-- name: weekend_vs_weekday
SELECT d.is_weekend, COUNT(*) AS orders, ROUND(SUM(o.net_amount), 2) AS net_revenue,
       ROUND(AVG(o.gross_amount), 2) AS avg_order_value
FROM fact_orders o JOIN dim_date d ON d.date = o.order_date
GROUP BY 1 ORDER BY 1;

-- name: refund_rate_by_category
SELECT category,
       COUNT(*) AS orders,
       ROUND(SUM(CASE WHEN refund_amount > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS refund_rate
FROM fact_orders GROUP BY category ORDER BY refund_rate DESC;
