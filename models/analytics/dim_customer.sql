-- Lesson-0106a: Get columns
SELECT 
  customer_id AS customer_key
  , customer_name AS customer_name
FROM `vit-lam-data.wide_world_importers.sales__customers`

------------------------------------------------------------

-- Original
-- SELECT 
--   *
-- FROM `vit-lam-data.wide_world_importers.sales__customers`