-- Original
-- SELECT 
--   *
-- FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`

-- Lesson 0103a: Dim table
SELECT 
 stock_item_id AS product_key  
 , stock_item_name AS product_name 
 , brand AS brand_name   
FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
