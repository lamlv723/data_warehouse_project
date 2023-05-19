WITH dim_supplier__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column AS (
  SELECT
    supplier_id AS supplier_key
    , supplier_name AS supplier_name
    , supplier_category_id AS supplier_category_key
  FROM dim_supplier__source
)

, dim_supplier__cast_type AS (
  SELECT
    CAST ( supplier_key AS INTEGER ) AS supplier_key
    , CAST ( supplier_name AS STRING ) AS supplier_name
    , CAST ( supplier_category_key AS INTEGER ) AS supplier_category_key
  FROM dim_supplier__rename_column
)

SELECT
  dim_supplier.supplier_key
  , dim_supplier.supplier_name
  , dim_supplier.supplier_category_key
  , COALESCE ( dim_supplier_category.supplier_category_name, 'Invalid' ) AS supplier_category_name
FROM dim_supplier__cast_type AS dim_supplier
LEFT JOIN {{ ref ('stg_dim_supplier_category') }} AS dim_supplier_category
ON dim_supplier.supplier_category_key = dim_supplier_category.supplier_category_key