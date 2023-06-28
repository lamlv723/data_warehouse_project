WITH fact_target_salesperson__target_source AS (
  SELECT
    year_month
    , salesperson_person_key
    , target_gross_amount
  FROM {{ref('stg_fact_target_salesperson')}}
)

, fact_target_salesperson__acctual_source AS (
  SELECT
    DATE_TRUNC(order_date, MONTH) AS year_month
    , salesperson_person_key
    , SUM(gross_amount) AS gross_amount
  FROM {{ref('fact_sales_order_line')}}
  GROUP BY 1, 2
)