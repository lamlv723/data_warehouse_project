-- Lesson-0105: Create data layer
WITH fact_sales_order_line__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_column AS (
  SELECT
    order_line_id AS sales_order_line_key
    , order_id AS sales_order_key
    , stock_item_id AS product_key
    , quantity AS quantity
    , unit_price AS unit_price
  FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS (
  SELECT
    CAST ( sales_order_line_key AS INTEGER ) AS sales_order_line_key
    , CAST ( sales_order_key AS INTEGER ) AS sales_order_key
    , CAST ( product_key AS INTEGER ) AS product_key
    , CAST ( quantity AS INTEGER ) AS quantity
    , CAST ( unit_price AS NUMERIC ) AS unit_price
  FROM fact_sales_order_line__rename_column
)

, fact_sales_order_line__calculated_measure_gross_amount AS (
  SELECT
  *
  , quantity * unit_price AS gross_amount
  FROM fact_sales_order_line__cast_type
)

SELECT
  fact_line.sales_order_line_key
  , fact_line.sales_order_key
  , fact_line.product_key
  , fact_header.customer_key -- Processed in stg model
  , fact_line.quantity
  , fact_line.unit_price
  , fact_line.gross_amount
FROM fact_sales_order_line__calculated_measure_gross_amount AS fact_line
-- Lesson-0106b: dbt ref
LEFT JOIN {{ ref ('stg_fact_sales_order') }} AS fact_header 
ON fact_line.sales_order_key = fact_header.sales_order_key

------------------------------------------------------------

-- Lesson-0106b: JOIN staging table
-- .....
-- LEFT JOIN `dbt-project-382304.wide_world_importers_dwh_staging.stg_fact_sales_order` AS fact_header
-- USING (sales_order_key)

------------------------------------------------------------

-- Lesson-0104b: Cast data type
-- SELECT
--   CAST ( order_line_id AS INTEGER ) AS sales_order_line_key
--   , CAST ( stock_item_id AS INTEGER ) AS product_key
--   , CAST ( quantity AS INTEGER ) AS quantity
--   , CAST ( unit_price AS NUMERIC ) AS unit_price
--   , CAST ( quantity AS INTEGER ) * CAST ( unit_price AS NUMERIC ) AS gross_amount
-- FROM `vit-lam-data.wide_world_importers.sales__order_lines`

------------------------------------------------------------

--- Lesson-0101
-- SELECT 
--   order_line_id AS sales_order_line_key
--   , quantity	AS quantity
--   , unit_price	AS unit_price
--   , quantity * unit_price AS gross_amount --Lesson 0102: Calculated measure
--   , stock_item_id AS product_key --Lesson 0103b
-- FROM `vit-lam-data.wide_world_importers.sales__order_lines`

------------------------------------------------------------

-- Original
-- SELECT 
--   *
-- FROM `vit-lam-data.wide_world_importers.sales__order_lines`