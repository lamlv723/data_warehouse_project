-- Lesson-0105: Create data layer
WITH fact_sales_order_line__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_column AS (
  SELECT
    order_line_id AS sales_order_line_key
    , description AS description
    , order_id AS sales_order_key
    , stock_item_id AS product_key
    , package_type_id AS package_type_key
    , quantity AS quantity
    , unit_price AS unit_price
    , tax_rate AS tax_rate
    , picked_quantity AS picked_quantity
  FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS (
  SELECT
    CAST ( sales_order_line_key AS INTEGER ) AS sales_order_line_key
    , CAST ( description AS STRING ) AS description
    , CAST ( sales_order_key AS INTEGER ) AS sales_order_key
    , CAST ( product_key AS INTEGER ) AS product_key
    , CAST ( package_type_key AS INTEGER ) AS package_type_key
    , CAST ( quantity AS INTEGER ) AS quantity
    , CAST ( unit_price AS NUMERIC ) AS unit_price
    , CAST ( tax_rate AS NUMERIC ) AS tax_rate
    , CAST ( picked_quantity AS INTEGER ) AS picked_quantity
  FROM fact_sales_order_line__rename_column
)

, fact_sales_order_line__add_calculated_measure AS (
  SELECT
  *
  , quantity * unit_price AS gross_amount
  , ( quantity * unit_price ) * ( tax_rate / 100 ) AS tax_amount
  FROM fact_sales_order_line__cast_type
)

SELECT
  fact_line.sales_order_line_key
  , fact_line.description
  , fact_line.product_key
  , fact_line.package_type_key
  , fact_line.sales_order_key
  , COALESCE ( fact_header.customer_key, -1 ) AS customer_key
  , COALESCE ( fact_header.salesperson_person_key, -1 ) AS salesperson_person_key
  , COALESCE ( fact_header.picked_by_person_key, -1 ) AS picked_by_person_key
  , COALESCE ( fact_header.contact_person_key, -1 ) AS contact_person_key
  , fact_header.order_date
  , fact_header.expected_delivery_date
  , fact_line.quantity
  , fact_line.unit_price
  , fact_line.tax_rate
  , fact_line.picked_quantity
  , fact_line.gross_amount
  , fact_line.tax_amount
FROM fact_sales_order_line__add_calculated_measure AS fact_line

LEFT JOIN {{ ref ('stg_fact_sales_order') }} AS fact_header
ON fact_line.sales_order_key = fact_header.sales_order_key