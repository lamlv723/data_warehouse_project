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
    , PERCENT_RANK() OVER(ORDER BY sales_amount) AS sales_amount_percentile
    , PERCENT_RANK() OVER(ORDER BY lifetime_sales_amount) AS lifetime_sales_amount_percentile
  FROM fact_customer_snapshot_by_month__calculate_cumulative
)

SELECT * FROM fact_customer_snapshot_by_month__calculate_percentile ORDER BY 2, 1 LIMIT 1000