-- Lesson-0105: Create data layer
WITH dim_product__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename_column AS (
  SELECT
    stock_item_id AS product_key
    , stock_item_name AS product_name
    , brand AS brand_name
  FROM dim_product__source
)

, dim_product__cast_type AS (
  SELECT
    CAST ( stock_item_id AS INTEGER ) AS product_key
    , CAST ( stock_item_name AS STRING ) AS product_name
    , CAST ( brand AS STRING ) AS brand_name
  FROM dim_product__source
)

SELECT
  product_key
  , product_name
  , brand_name
FROM dim_product__cast_type

------------------------------------------------------------

-- Lesson-0104a: Cast data type
-- SELECT
--  CAST ( stock_item_id AS INTEGER ) AS product_key  
--  , CAST ( stock_item_name AS STRING ) AS product_name 
--  , CAST ( brand AS STRING ) AS brand_name   
-- FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`

------------------------------------------------------------

-- Lesson-0103a: Dim table
-- SELECT
--  stock_item_id AS product_key
--  , stock_item_name AS product_name
--  , brand AS brand_name
-- FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`

------------------------------------------------------------

-- Original
-- SELECT
--   *
-- FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`