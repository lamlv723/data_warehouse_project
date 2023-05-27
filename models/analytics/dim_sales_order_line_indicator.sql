WITH dim_is_undersupply_backordered AS (
  SELECT
    TRUE AS is_undersupply_backordered_boolean
    , "Undersupply Backordered" AS is_undersupply_backordered

  UNION ALL

  SELECT
    FALSE AS is_undersupply_backordered_boolean
    , "Not Undersupply Backordered" AS is_undersupply_backordered
)

SELECT *
FROM dim_is_undersupply_backordered
CROSS JOIN {{ ref ('dim_package_type') }} AS dim_package_type
ORDER BY 1, 3