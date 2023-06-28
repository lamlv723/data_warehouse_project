WITH fact_target_salesperson__source AS (
  SELECT
    year_month
    , salesperson_person_key
    , target_gross_amount
  FROM {{ ref ( 'stg_fact_target_salesperson' )}}
)
