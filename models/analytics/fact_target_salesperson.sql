WITH fact_target_salesperson__target_source AS (
  SELECT
    year_month
    , salesperson_person_key
    , target_gross_amount
  FROM {{ref('stg_fact_target_salesperson')}}
)

, fact_target_salesperson__actual_source AS (
  SELECT
    DATE_TRUNC(order_date, MONTH) AS year_month
    , salesperson_person_key
    , SUM(gross_amount) AS gross_amount
  FROM {{ref('fact_sales_order_line')}}
  GROUP BY 1, 2
)

, fact_target_salesperson__combine AS (
  SELECT
    year_month
    , salesperson_person_key
    , COALESCE(fact_target.target_gross_amount, 0) AS target_gross_amount
    , COALESCE(fact_actual.gross_amount, 0) AS gross_amount
  FROM fact_target_salesperson__target_source AS fact_target
  FULL OUTER JOIN fact_target_salesperson__actual_source AS fact_actual
    USING(year_month, salesperson_person_key)
)

, fact_target_salesperson__add_achievement_ratio AS (
  SELECT
    *
    , gross_amount / target_gross_amount AS achievement_ratio
  FROM fact_target_salesperson__combine
)

SELECT
  *
  , CASE
      WHEN achievement_ratio > 0.9 THEN 'Achieved'
      WHEN achievement_ratio BETWEEN 0 AND 0.9 THEN 'Not Achieved'
      ELSE 'Undefined'
    END AS is_achieved
FROM fact_target_salesperson__add_achievement_ratio
ORDER BY 1, 2