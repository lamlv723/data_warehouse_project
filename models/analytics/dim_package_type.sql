WITH dim_package_type__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)

, dim_package_type__rename_column AS (
  SELECT
    package_type_id AS package_type_key
    , package_type_name AS package_type_name
  FROM dim_package_type__source
)

, dim_package_type__cast_type AS (
  SELECT
    CAST ( package_type_key AS INTEGER ) AS package_type_key
    , CAST ( package_type_name AS STRING ) AS package_type_name
  FROM dim_package_type__rename_column
)

, dim_package_type_add_undefined_record AS (
  SELECT
    package_type_key
    , package_type_name
  FROM dim_package_type__cast_type

  UNION ALL
  SELECT
    0 AS package_type_key
    , 'Undefined' AS package_type_name

  -- As this table does NOT used to flatten. So, no need to add 'Invalid'
  -- UNION ALL
  -- SELECT
  --   -1 AS package_type_key
  --   , 'Invalid' AS package_type_name
)

, dim_package_type_handle_null AS (
  SELECT
    package_type_key
    , COALESCE ( package_type_name, 'Undefined' ) AS package_type_name
  FROM dim_package_type_add_undefined_record
)

SELECT
  package_type_key
  , package_type_name
FROM dim_package_type_handle_null
ORDER BY package_type_key