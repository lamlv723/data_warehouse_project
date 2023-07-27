WITH dim_customer_attribute__summarize AS (
  SELECT
    customer_key
    , SUM(gross_amount) AS lifetime_sales_amount
    , COUNT(DISTINCT(sales_order_key)) AS lifetime_sales_order
    , SUM(
        CASE
          WHEN order_date BETWEEN DATE_TRUNC('2016-05-31', MONTH ) - INTERVAL 12 MONTH AND '2016-05-31' THEN gross_amount
        END
      ) AS last_12months_sales_amount
    , COUNT(
        DISTINCT(
          CASE
            WHEN order_date BETWEEN DATE_TRUNC('2016-05-31', MONTH ) - INTERVAL 12 MONTH AND '2016-05-31' THEN sales_order_key
          END
        )
      ) AS last_12months_sales_order
    , DATE_TRUNC(MIN(order_date), MONTH) AS start_month
    , DATE_TRUNC(MAX(order_date), MONTH) AS end_month
  FROM {{ref('fact_sales_order_line')}}
  GROUP BY 1
)

, dim_customer_attribute__calculate_percentile AS (
  SELECT
    *
    , PERCENT_RANK() OVER(ORDER BY(lifetime_sales_amount)) AS lifetime_sales_amount_percentile
    , PERCENT_RANK() OVER(ORDER BY(last_12months_sales_amount)) AS last_12months_sales_amount_percentile
  FROM dim_customer_attribute__summarize
)

, dim_customer_attribute__segment_percentile AS (
  SELECT
    *
    , CASE
        WHEN lifetime_sales_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN lifetime_sales_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN lifetime_sales_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
        ELSE 'Invalid'
      END AS lifetime_sales_amount_segment
    , CASE
        WHEN last_12months_sales_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN last_12months_sales_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN last_12months_sales_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
        ELSE 'Invalid'
      END AS last_12months_sales_amount_segment
  FROM dim_customer_attribute__calculate_percentile
)

SELECT
  customer_key
  , lifetime_sales_amount
  , lifetime_sales_order
  , last_12months_sales_amount
  , last_12months_sales_order
  , lifetime_sales_amount_percentile
  , last_12months_sales_amount_percentile
  , lifetime_sales_amount_segment
  , last_12months_sales_amount_segment
  , start_month
  , end_month
FROM dim_customer_attribute__segment_percentile