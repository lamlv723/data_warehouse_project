WITH dim_customer__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_column AS (
  SELECT
    customer_id AS customer_key
    , customer_name AS customer_name
    , is_statement_sent AS is_statement_sent_boolean
    , is_on_credit_hold AS is_on_credit_hold_boolean
    , customer_category_id AS customer_category_key
    , buying_group_id AS buying_group_key
    , delivery_city_id AS delivery_city_key
  FROM dim_customer__source
)

, dim_customer__cast_type AS (
  SELECT
    CAST ( customer_key AS INTEGER ) AS customer_key
    , CAST ( customer_name AS STRING ) AS customer_name
    , CAST ( is_statement_sent_boolean AS BOOLEAN ) AS is_statement_sent_boolean
    , CAST ( is_on_credit_hold_boolean AS BOOLEAN ) AS is_on_credit_hold_boolean
    , CAST ( customer_category_key AS INTEGER ) AS customer_category_key
    , CAST ( buying_group_key AS INTEGER ) AS buying_group_key
    , CAST ( delivery_city_key AS INTEGER ) AS delivery_city_key
  FROM dim_customer__rename_column
)

, dim_customer__convert_boolean_to_string AS (
  SELECT
    *
    , CASE
        WHEN is_statement_sent_boolean IS TRUE THEN 'Sent'
        WHEN is_statement_sent_boolean IS FALSE THEN 'Not Sent'
        WHEN is_statement_sent_boolean IS NULL THEN 'Undefined'
        ElSE 'Invalid' END
      AS is_statement_sent
    , CASE
        WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Hold'
        WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Hold'
        WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
        ElSE 'Invalid' END
      AS is_on_credit_hold
  FROM dim_customer__cast_type
)

, dim_customer__handle_null AS (
  SELECT
    customer_key
    , customer_name
    , is_statement_sent
    , is_on_credit_hold
    , customer_category_key
    , COALESCE ( buying_group_key, 0 ) AS buying_group_key
    , delivery_city_key 
  FROM dim_customer__convert_boolean_to_string
)

, dim_customer__add_undefined_record AS (
  SELECT
  customer_key
  , customer_name
  , is_statement_sent
  , is_on_credit_hold
  , customer_category_key
  , buying_group_key 
  , delivery_city_key 
  FROM dim_customer__handle_null

  UNION ALL
  SELECT
  0 AS customer_key
  , 'Undefined' AS customer_name
  , 'Undefined' AS is_statement_sent
  , 'Undefined' AS is_on_credit_hold
  , 0 AS customer_category_key
  , 0 AS buying_group_key
  , 0 AS delivery_city_key

  UNION ALL
  SELECT
  -1 AS customer_key
  , 'Invalid' AS customer_name
  , 'Invalid' AS is_statement_sent
  , 'Invalid' AS is_on_credit_hold
  , -1 AS customer_category_key
  , -1 AS buying_group_key
  , -1 AS delivery_city_key
)

SELECT
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_statement_sent
  , dim_customer.is_on_credit_hold
  , dim_customer.customer_category_key
  , COALESCE ( dim_customer_category.customer_category_name, 'Invalid' ) AS customer_category_name
  , dim_customer.buying_group_key
  , COALESCE ( dim_buying_group.buying_group_name, 'Invalid' ) AS buying_group_name
  , dim_customer.delivery_city_key
  , COALESCE ( dim_city.city_name, 'Invalid' ) AS delivery_city_name
  , COALESCE ( dim_city.state_province_name, 'Invalid' ) AS delivery_state_province_name
  , COALESCE ( dim_city.country_name, 'Invalid' ) AS delivery_country_name
FROM dim_customer__add_undefined_record AS dim_customer
LEFT JOIN {{ ref ('stg_dim_customer_category') }} AS dim_customer_category  -- Flatten dim_customer_category to dim_customer
ON dim_customer.customer_category_key = dim_customer_category.customer_category_key
LEFT JOIN {{ ref ('stg_dim_buying_group') }} AS dim_buying_group  -- Flatten dim_buying_group to dim_customer
ON dim_customer.buying_group_key = dim_buying_group.buying_group_key
LEFT JOIN {{ ref ('stg_dim_city') }} AS dim_city -- Flatten delivery city
ON dim_customer.delivery_city_key = dim_city.city_key