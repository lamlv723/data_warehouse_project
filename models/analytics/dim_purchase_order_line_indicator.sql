WITH dim_is_order_line_finalized AS (
  SELECT
    TRUE AS is_order_line_finalized_boolean
    , "Finalized" AS is_order_line_finalized

  UNION ALL

  SELECT
    FALSE AS is_order_line_finalized_boolean
    , "Not Finalized" AS is_order_line_finalized
)

 , dim_is_order_finalized AS (
  SELECT
    TRUE AS is_order_finalized_boolean
    , "Finalized" AS is_order_finalized

  UNION ALL

  SELECT
    FALSE AS is_order_finalized_boolean
    , "Not Finalized" AS is_order_finalized
)


SELECT
  FARM_FINGERPRINT ( 
    CONCAT ( 
      dim_is_order_finalized.is_order_finalized
      , ','
      , dim_is_order_line_finalized.is_order_line_finalized
      , ','
      , dim_package_type.package_type_key 
    ) 
  ) AS purchase_order_line_indicator_key
  , dim_is_order_finalized.is_order_finalized_boolean
  , dim_is_order_finalized.is_order_finalized
  , dim_is_order_line_finalized.is_order_line_finalized_boolean
  , dim_is_order_line_finalized.is_order_line_finalized
  , dim_package_type.package_type_key
  , dim_package_type.package_type_name
FROM dim_is_order_line_finalized

CROSS JOIN {{ ref ('dim_package_type') }} AS dim_package_type

CROSS JOIN dim_is_order_finalized