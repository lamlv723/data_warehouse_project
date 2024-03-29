-- Lesson-0106b: Create staging table
WITH sales_order__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, sales_order__rename_column AS (
  SELECT
    order_id AS sales_order_key
    , is_undersupply_backordered AS is_undersupply_backordered
    , customer_id AS customer_key
    , salesperson_person_id AS salesperson_person_key
    , picked_by_person_id AS picked_by_person_key
    , contact_person_id AS contact_person_key
    , order_date AS order_date
    , expected_delivery_date AS expected_delivery_date
  FROM sales_order__source
)

, sales_order__cast_type AS (
  SELECT
    CAST ( sales_order_key AS INTEGER ) AS sales_order_key
    , CAST ( is_undersupply_backordered AS BOOLEAN ) AS is_undersupply_backordered_boolean
    , CAST ( customer_key AS INTEGER ) AS customer_key
    , CAST ( salesperson_person_key AS INTEGER ) AS salesperson_person_key
    , CAST ( picked_by_person_key AS INTEGER ) AS picked_by_person_key
    , CAST ( contact_person_key AS INTEGER ) AS contact_person_key
    , CAST ( order_date AS DATE ) AS order_date
    , CAST ( expected_delivery_date AS DATE ) AS expected_delivery_date
  FROM sales_order__rename_column
)

, sales_order__convert_boolean AS (
  SELECT *
  , CASE
      WHEN  is_undersupply_backordered_boolean IS TRUE THEN 'Undersupply Backordered'
      WHEN  is_undersupply_backordered_boolean IS FALSE THEN 'Not Undersupply Backordered'
      ELSE 'Undefined'
    END AS is_undersupply_backordered
  FROM sales_order__cast_type
)

SELECT
  sales_order_key
  , COALESCE ( customer_key, 0 ) AS customer_key
  , is_undersupply_backordered
  , COALESCE ( salesperson_person_key, 0 ) AS salesperson_person_key
  , COALESCE ( picked_by_person_key, 0 ) AS picked_by_person_key
  , COALESCE ( contact_person_key, 0 ) AS contact_person_key
  , order_date -- Lesson-0111a
  , expected_delivery_date
FROM sales_order__convert_boolean