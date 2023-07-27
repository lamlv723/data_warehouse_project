WITH fact_customer_snapshot_by_month__summarize AS (
  SELECT
    DATE_TRUNC(order_date, MONTH) AS year_month
    , customer_key
    , SUM(gross_amount) AS sales_amount
  FROM {{ref('fact_sales_order_line')}}
  GROUP BY 1, 2
)

, fact_customer_snapshot_by_month__get_unique_customer AS (
  SELECT DISTINCT customer_key
  FROM fact_customer_snapshot_by_month__summarize
  )

, fact_customer_snapshot_by_month__get_unique_month AS (
  SELECT DISTINCT year_month
  FROM fact_customer_snapshot_by_month__summarize
  )

, fact_customer_snapshot_by_month__filter_start_end_month AS (
  SELECT
    fact_customer_snapshot_by_month__get_unique_month.year_month
    , dim_customer_attribute.customer_key
    , dim_customer_attribute.start_month
    , dim_customer_attribute.end_month
  FROM {{ref('dim_customer_attribute')}} AS dim_customer_attribute
  CROSS JOIN fact_customer_snapshot_by_month__get_unique_month
  WHERE 
    fact_customer_snapshot_by_month__get_unique_month.year_month
    BETWEEN dim_customer_attribute.start_month AND dim_customer_attribute.end_month
  ORDER BY 2, 1
)

, fact_customer_snapshot_by_month__dense_customer_month AS (
  SELECT
    fact_customer_snapshot_by_month__filter_start_end_month.year_month
    , fact_customer_snapshot_by_month__get_unique_customer.customer_key
  FROM fact_customer_snapshot_by_month__filter_start_end_month
  LEFT JOIN fact_customer_snapshot_by_month__get_unique_customer
  USING (customer_key)
  ORDER BY 2, 1
)

, fact_customer_snapshot_by_month__dense AS (
  SELECT
    year_month
    , customer_key
    , COALESCE(fact_customer_snapshot_by_month__summarize.sales_amount, 0) AS sales_amount
  FROM fact_customer_snapshot_by_month__dense_customer_month
  LEFT JOIN fact_customer_snapshot_by_month__summarize
  USING (year_month, customer_key)
  ORDER BY 2, 1
)

, fact_customer_snapshot_by_month__calculate_cumulative AS (
  SELECT
    *
    , SUM(sales_amount) OVER(PARTITION BY customer_key ORDER BY year_month) AS lifetime_sales_amount
    , LAG(sales_amount, 1) OVER(PARTITION BY customer_key ORDER BY year_month) AS last_month_sales_amount
    , SUM(sales_amount) OVER(PARTITION BY customer_key ORDER BY year_month) AS last_12months_sales_amount
  FROM fact_customer_snapshot_by_month__dense
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
      END AS sales_amount_monetary
    , CASE
        WHEN lifetime_sales_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN lifetime_sales_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN lifetime_sales_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
        ELSE 'Invalid'
      END AS lifetime_sales_amount_monetary
  FROM fact_customer_snapshot_by_month__calculate_percentile
)

SELECT
  year_month
  , customer_key
  , sales_amount
  , lifetime_sales_amount
  , last_month_sales_amount
  , last_12months_sales_amount
  , sales_amount_monetary
  , lifetime_sales_amount_monetary
FROM fact_customer_snapshot_by_month__percentile_segment
ORDER BY 2, 1