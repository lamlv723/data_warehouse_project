WITH dim_customer_attribute__summarize AS (
  SELECT
    customer_key
    , SUM(gross_amount) AS lifetime_sales_amount
    , COUNT(DISTINCT(sales_order_key)) AS lifetime_sales_order
    , SUM(
        CASE
          WHEN order_date BETWEEN DATE_TRUNC('2016-05-31', MONTH ) - INTERVAL 12 MONTH AND '2016-05-31' THEN gross_amount
        END
      ) AS L12MTD_sales_amount
    , COUNT(
        DISTINCT(
          CASE
            WHEN order_date BETWEEN DATE_TRUNC('2016-05-31', MONTH ) - INTERVAL 12 MONTH AND '2016-05-31' THEN sales_order_key
          END
        )
      ) AS L12TD_sales_order
  FROM {{ref('fact_sales_order_line')}}
  GROUP BY 1
)

, dim_customer_attribute__calculate_percentile AS (
  SELECT
    *
    , PERCENT_RANK() OVER(ORDER BY(lifetime_sales_amount)) AS lifetime_sales_amount_percentile
    , PERCENT_RANK() OVER(ORDER BY(L12MTD_sales_amount)) AS L12MTD_sales_amount_percentile
  FROM dim_customer_attribute__summarize
)

, dim_customer_attribute__segment_percentile AS (
  SELECT
    *
    , CASE
        WHEN lifetime_sales_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN lifetime_sales_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN lifetime_sales_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
        ELSE 'Undefined'
      END AS lifetime_sales_amount_segment
    , CASE
        WHEN L12MTD_sales_amount_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN L12MTD_sales_amount_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN L12MTD_sales_amount_percentile BETWEEN 0 AND 0.5 THEN 'Low'
        ELSE 'Undefined'
      END AS L12MTD_sales_amount_segment
  FROM dim_customer_attribute__calculate_percentile
)

SELECT * FROM dim_customer_attribute__segment_percentile