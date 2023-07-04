WITH fact_customer_snapshot_by_month__summarize AS (
  SELECT
    DATE_TRUNC(order_date, MONTH) AS year_month
    , customer_key
    , SUM(gross_amount) AS sales_amount
  FROM {{ref('fact_sales_order_line')}}
  GROUP BY 1, 2
)

, fact_customer_snapshot_by_month__calculate_cumulative AS (
  SELECT
    *
    , SUM(sales_amount) OVER(PARTITION BY customer_key ORDER BY year_month) AS lifetime_sales_amount
  FROM fact_customer_snapshot_by_month__summarize
)

, fact_customer_snapshot_by_month__calculate_percentile AS (
  SELECT
    *
    , PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY sales_amount) AS sales_amount_percentile
    , PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY lifetime_sales_amount) AS lifetime_sales_amount_percentile
  FROM fact_customer_snapshot_by_month__calculate_cumulative
)

, fact_customer_snapshot_by_month__percentile_segment AS (
  SELECT
    *
    , CASE
        WHEN sales_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN sales_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN sales_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
        ELSE 'Invalid'
      END AS sales_amount_segment
    , CASE
        WHEN lifetime_sales_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN lifetime_sales_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN lifetime_sales_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
        ELSE 'Invalid'
      END AS lifetime_sales_amount_segment
  FROM fact_customer_snapshot_by_month__calculate_percentile
)

SELECT
  year_month
  , customer_key
  , sales_amount
  , lifetime_sales_amount
  -- , sales_amount_percentile
  -- , lifetime_sales_amount_percentile
  , sales_amount_segment
  , lifetime_sales_amount_segment
FROM fact_customer_snapshot_by_month__percentile_segment