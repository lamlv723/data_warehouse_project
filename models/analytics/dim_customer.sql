WITH dim_customer__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_column AS (
  SELECT
    customer_id AS customer_key
    , customer_name AS customer_name
    -- Lesson-0107c: Flatten dim_customer
    , customer_category_id AS customer_category_key
    , buying_group_id AS buying_group_key
  FROM dim_customer__source
)

, dim_customer__cast_type AS (
  SELECT
    CAST ( customer_key AS INTEGER) AS customer_key
    , CAST ( customer_name AS STRING) AS customer_name
    , CAST ( customer_category_key AS INTEGER ) AS customer_category_key
    , CAST ( buying_group_key AS INTEGER ) AS buying_group_key
  FROM dim_customer__rename_column
)

SELECT
  customer_key
  , customer_name
  , customer_category_key
  , buying_group_key
FROM dim_customer__cast_type

------------------------------------------------------------

-- Lesson-0106a: Get columns
-- SELECT 
--   customer_id AS customer_key
--   , customer_name AS customer_name
-- FROM `vit-lam-data.wide_world_importers.sales__customers`

------------------------------------------------------------

-- Original
-- SELECT 
--   *
-- FROM `vit-lam-data.wide_world_importers.sales__customers`