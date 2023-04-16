-- Lesson-0104b: Cast data type
SELECT 
  CAST ( order_line_id AS INTEGER ) AS sales_order_line_key
  , CAST ( stock_item_id AS INTEGER ) AS product_key
  , CAST ( quantity AS INTEGER ) AS quantity
  , CAST ( unit_price AS NUMERIC ) AS unit_price
  , CAST ( quantity AS INTEGER ) * CAST ( unit_price AS NUMERIC ) AS gross_amount
FROM `vit-lam-data.wide_world_importers.sales__order_lines`

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