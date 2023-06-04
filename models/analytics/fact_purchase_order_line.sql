WITH fact_purchase_order_line__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines`
)

, fact_purchase_order_line__rename_column AS (
  SELECT
    purchase_order_line_id AS purchase_order_line_key
    , description AS description
    , ordered_outers AS ordered_outers
    , received_outers AS received_outers
    , is_order_line_finalized AS is_order_line_finalized
    , stock_item_id AS product_key
    , package_type_id AS package_type_key
    , purchase_order_id AS purchase_order_key
  FROM fact_purchase_order_line__source
)

, fact_purchase_order_line__cast_type AS (
  SELECT
    CAST ( purchase_order_line_key AS INTEGER ) AS purchase_order_line_key
    , CAST ( description AS STRING ) AS description
    , CAST ( ordered_outers AS INTEGER ) AS ordered_outers
    , CAST ( received_outers AS INTEGER ) AS received_outers
    , CAST ( is_order_line_finalized AS BOOLEAN ) AS is_order_line_finalized_boolean
    , CAST ( product_key AS INTEGER ) AS product_key
    , CAST ( package_type_key AS INTEGER ) AS package_type_key
    , CAST ( purchase_order_key AS INTEGER ) AS purchase_order_key
  FROM fact_purchase_order_line__rename_column
)

, fact_purchase_order_line__convert_boolean AS (
  SELECT
  *
  , CASE 
      WHEN is_order_line_finalized_boolean IS TRUE THEN 'Finalized'
      WHEN is_order_line_finalized_boolean IS FALSE THEN 'Not Finalized'
      ELSE 'Undefined'
    END AS is_order_line_finalized
  FROM fact_purchase_order_line__cast_type
)

SELECT
  fact_line.purchase_order_line_key
  , fact_line.description
  , fact_line.ordered_outers
  , fact_line.received_outers
  -- , fact_line.is_order_line_finalized
  -- , fact_header.is_order_finalized
  , fact_line.product_key
  , fact_line.package_type_key
  , FARM_FINGERPRINT (
      CONCAT (
        COALESCE ( fact_header.is_order_finalized, 'Invalid' )
        , ','
        , fact_line.is_order_line_finalized
        , ','
        , fact_line.package_type_key
      )
    ) AS purchase_order_line_indicator_key
  , fact_line.purchase_order_key
  , COALESCE ( fact_header.supplier_key, -1 ) AS supplier_key -- Handle null due to left join
  , COALESCE ( fact_header.delivery_method_key, -1 ) AS delivery_method_key
  , COALESCE ( fact_header.contact_person_key, -1 ) AS contact_person_key
  , fact_header.order_date
  , fact_header.expected_delivery_date
FROM fact_purchase_order_line__convert_boolean AS fact_line

LEFT JOIN {{ ref ('stg_fact_purchase_order') }} AS fact_header
ON fact_line.purchase_order_key = fact_header.purchase_order_key