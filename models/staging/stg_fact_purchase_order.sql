-- Lesson-0106b: Create staging table
WITH purchase_order__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__purchase_orders`
)

, purchase_order__rename_column AS (
  SELECT
    purchase_order_id AS purchase_order_key
    , is_order_finalized AS is_order_finalized
    , supplier_id AS supplier_key
    , delivery_method_id AS delivery_method_key
    , contact_person_id AS contact_person_key
    , order_date AS order_date
    , expected_delivery_date AS expected_delivery_date
  FROM purchase_order__source
)

, purchase_order__cast_type AS (
  SELECT
    CAST ( purchase_order_key AS INTEGER ) AS purchase_order_key
    , CAST ( is_order_finalized AS BOOLEAN ) AS is_order_finalized_boolean
    , CAST ( supplier_key AS INTEGER ) AS supplier_key
    , CAST ( delivery_method_key AS INTEGER ) AS delivery_method_key
    , CAST ( contact_person_key AS INTEGER ) AS contact_person_key
    , CAST ( order_date AS DATE ) AS order_date
    , CAST ( expected_delivery_date AS DATE ) AS expected_delivery_date
  FROM purchase_order__rename_column
)

, purchase_order__convert_boolean AS (
  SELECT *
  , CASE
      WHEN  is_order_finalized_boolean IS TRUE THEN 'Finalized'
      WHEN  is_order_finalized_boolean IS FALSE THEN 'Not Finalized'
      ELSE 'Undefined'
    END AS is_order_finalized
  FROM purchase_order__cast_type
)

SELECT
  purchase_order_key
  , is_order_finalized
  , supplier_key
  , delivery_method_key
  , contact_person_key
  , order_date
  , expected_delivery_date
FROM purchase_order__convert_boolean