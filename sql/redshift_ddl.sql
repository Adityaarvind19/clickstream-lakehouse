-- Reference DDL for loading the gold layer into Amazon Redshift.
-- NOT deployed as part of this repo (it runs locally on DuckDB); included to show the
-- physical design choices you would make on Redshift.

CREATE TABLE dim_user (
  user_sk      CHAR(16)    NOT NULL,
  user_id      VARCHAR(16) NOT NULL,
  plan_tier    VARCHAR(8),
  country      CHAR(2),
  signup_date  DATE,
  valid_from   DATE        NOT NULL,
  valid_to     DATE        NOT NULL,
  is_current   BOOLEAN     NOT NULL,
  PRIMARY KEY (user_sk)
) DISTSTYLE ALL;                                   -- small dimension: replicate to every node

CREATE TABLE dim_product (
  product_id  VARCHAR(8) NOT NULL PRIMARY KEY,
  category    VARCHAR(16),
  base_price  DECIMAL(10,2)
) DISTSTYLE ALL;

CREATE TABLE fact_orders (
  order_id       VARCHAR(16) NOT NULL,
  user_sk        CHAR(16)    REFERENCES dim_user(user_sk),
  user_id        VARCHAR(16),
  product_id     VARCHAR(8)  REFERENCES dim_product(product_id),
  category       VARCHAR(16),
  plan_tier      VARCHAR(8),
  device         VARCHAR(8),
  order_date     DATE        NOT NULL,
  order_ts       TIMESTAMP,
  quantity       SMALLINT,
  price          DECIMAL(10,2),
  gross_amount   DECIMAL(12,2),
  refund_amount  DECIMAL(12,2),
  refund_ts      TIMESTAMP,
  net_amount     DECIMAL(12,2)
)
DISTKEY (user_id)                                  -- co-locate a user's orders for per-user joins
SORTKEY (order_date);                              -- date-range scans skip blocks (zone maps)

-- Load straight from the S3 lake (Parquet is self-describing):
-- COPY fact_orders FROM 's3://<bucket>/lake/gold/fact_orders/'
--   IAM_ROLE '<role-arn>' FORMAT AS PARQUET;
