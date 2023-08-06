WITH dim_product__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename_column AS (
  SELECT
    stock_item_id AS product_key
    , supplier_id AS supplier_key
    , stock_item_name AS product_name
    , brand AS brand_name
    , size AS product_size
    , is_chiller_stock AS is_chiller_stock_boolean
    , color_id AS color_key
    , unit_package_id AS unit_package_type_key
    , outer_package_id AS outer_package_type_key
  FROM dim_product__source
)

, dim_product__cast_type AS (
  SELECT
    CAST ( product_key AS INTEGER ) AS product_key
    , CAST ( product_name AS STRING ) AS product_name
    , CAST ( brand_name AS STRING ) AS brand_name
    , CAST ( product_size AS STRING ) AS product_size
    , CAST ( is_chiller_stock_boolean AS BOOLEAN ) AS is_chiller_stock_boolean
    , CAST ( supplier_key AS INTEGER ) AS supplier_key
    , CAST ( color_key AS INTEGER ) AS color_key
    , CAST ( unit_package_type_key AS INTEGER ) AS unit_package_type_key
    , CAST ( outer_package_type_key AS INTEGER ) AS outer_package_type_key
  FROM dim_product__rename_column
)

, dim_product__convert_boolean_to_string AS (
  SELECT
    *
    , CASE
        WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
        WHEN is_chiller_stock_boolean IS FALSE THEN 'Not Chiller Stock'
        WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' END
      AS is_chiller_stock
  FROM dim_product__cast_type
)

, dim_product__handle_null AS (
  SELECT 
    product_key
    , product_name
    , COALESCE ( brand_name, 'Undefined' ) AS brand_name
    , COALESCE ( product_size, 'Undefined' ) AS product_size
    , is_chiller_stock
    , supplier_key
    , COALESCE ( color_key, 0 ) AS color_key
    , unit_package_type_key
    , outer_package_type_key
  FROM dim_product__convert_boolean_to_string
)

, dim_product__add_undefined_record AS (
  SELECT 
    product_key
    , product_name
    , brand_name
    , product_size
    , is_chiller_stock
    , supplier_key
    , color_key
    , unit_package_type_key
    , outer_package_type_key
  FROM dim_product__handle_null

  UNION ALL
  SELECT
    -1 AS product_key
    , 'Invalid' AS product_name
    , 'Invalid' AS brand_name
    , 'Invalid' AS product_size
    , 'Invalid' AS is_chiller_stock
    , -1 AS supplier_key
    , -1 AS color_key
    , -1 AS unit_package_type_key
    , -1 AS outer_package_type_key
  UNION ALL
  SELECT
    0 AS product_key
    , 'Undefined' AS product_name
    , 'Undefined' AS brand_name
    , 'Undefined' AS product_size
    , 'Undefined' AS is_chiller_stock
    , 0 AS supplier_key
    , 0 AS color_key
    , 0 AS unit_package_type_key
    , 0 AS outer_package_type_key
)

SELECT 
  dim_product.product_key
  , dim_product.product_name
  , brand_name
  , dim_product.is_chiller_stock
  , dim_product.supplier_key
  , COALESCE ( dim_supplier.supplier_name, 'Invalid' ) AS supplier_name
  , COALESCE ( dim_supplier_category.supplier_category_name, 'Invalid' ) AS supplier_category_name
  , dim_product.color_key
  , COALESCE ( dim_color.color_name, 'Invalid' ) AS color_name
  , unit_package_type_key
  , COALESCE ( dim_package_type_unit.package_type_name, 'Invalid' ) AS unit_package_type_name
  , outer_package_type_key
  , COALESCE ( dim_package_type_outer.package_type_name, 'Invalid' ) AS outer_package_type_name
  , COALESCE ( dim_product__external.category_key, -1 ) AS category_key
  , COALESCE ( dim_category.category_name, 'Invalid' ) AS category_name
  , COALESCE ( dim_category.category_level, -1 ) AS category_level
  , COALESCE ( dim_category.parent_category_key, -1 ) AS parent_category_key
  , COALESCE ( dim_category.parent_category_name, 'Invalid' ) AS parent_category_name
  , COALESCE ( dim_category.category_key_level_1, -1 ) AS category_key_level_1
  , COALESCE ( dim_category.category_level_1, 'Invalid' ) AS category_level_1
  , COALESCE ( dim_category.category_key_level_2, -1 ) AS category_key_level_2
  , COALESCE ( dim_category.category_level_2, 'Invalid' ) AS category_level_2
  , COALESCE ( dim_category.category_key_level_3, -1 ) AS category_key_level_3
  , COALESCE ( dim_category.category_level_3, 'Invalid' ) AS category_level_3
  , COALESCE ( dim_category.category_key_level_4, -1 ) AS category_key_level_4
  , COALESCE ( dim_category.category_level_4, 'Invalid' ) AS category_level_4
FROM dim_product__add_undefined_record AS dim_product

LEFT JOIN {{ ref ('dim_supplier') }} AS dim_supplier
ON dim_product.supplier_key = dim_supplier.supplier_key

LEFT JOIN {{ ref ('dim_supplier') }} AS dim_supplier_category
ON dim_product.supplier_key = dim_supplier_category.supplier_key

LEFT JOIN {{ ref ('stg_dim_color') }} AS dim_color
ON dim_product.color_key = dim_color.color_key

LEFT JOIN {{ ref ('dim_package_type') }} AS dim_package_type_unit
ON dim_product.unit_package_type_key = dim_package_type_unit.package_type_key

LEFT JOIN {{ ref ('dim_package_type') }} AS dim_package_type_outer
ON dim_product.outer_package_type_key = dim_package_type_outer.package_type_key

LEFT JOIN {{ ref ('stg_dim_product__external') }} AS dim_product__external
ON dim_product.product_key = dim_product__external.product_key

LEFT JOIN {{ ref ('dim_category') }} AS dim_category
ON dim_product__external.category_key = dim_category.category_key