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
FROM dim_product__handle_null AS dim_product
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