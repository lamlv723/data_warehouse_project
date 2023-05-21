WITH dim_customer__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_column AS (
  SELECT
    customer_id AS customer_key
    , customer_name AS customer_name
    , credit_limit AS credit_limit
    , standard_discount_percentage AS standard_discount_percentage
    , payment_days AS payment_days
    , is_statement_sent AS is_statement_sent_boolean
    , is_on_credit_hold AS is_on_credit_hold_boolean
    , customer_category_id AS customer_category_key
    , buying_group_id AS buying_group_key
    , delivery_city_id AS delivery_city_key
    , primary_contact_person_id AS primary_contact_person_key
    , alternate_contact_person_id AS alternate_contact_person_key
    , bill_to_customer_id AS bill_to_customer_key
  FROM dim_customer__source
)

, dim_customer__cast_type AS (
  SELECT
    CAST ( customer_key AS INTEGER ) AS customer_key
    , CAST ( customer_name AS STRING ) AS customer_name
    , CAST ( credit_limit AS NUMERIC ) AS credit_limit
    , CAST ( standard_discount_percentage AS NUMERIC ) AS standard_discount_percentage
    , CAST ( payment_days AS INTEGER ) AS payment_days
    , CAST ( is_statement_sent_boolean AS BOOLEAN ) AS is_statement_sent_boolean
    , CAST ( is_on_credit_hold_boolean AS BOOLEAN ) AS is_on_credit_hold_boolean
    , CAST ( customer_category_key AS INTEGER ) AS customer_category_key
    , CAST ( buying_group_key AS INTEGER ) AS buying_group_key
    , CAST ( delivery_city_key AS INTEGER ) AS delivery_city_key
    , CAST ( primary_contact_person_key AS INTEGER ) AS primary_contact_person_key
    , CAST ( alternate_contact_person_key AS INTEGER ) AS alternate_contact_person_key
    , CAST ( bill_to_customer_key AS INTEGER ) AS bill_to_customer_key
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
    , COALESCE ( credit_limit, 0 ) AS credit_limit
    , standard_discount_percentage
    , payment_days
    , is_statement_sent
    , is_on_credit_hold
    , customer_category_key
    , COALESCE ( buying_group_key, 0 ) AS buying_group_key
    , delivery_city_key
    , primary_contact_person_key
    , COALESCE ( alternate_contact_person_key, 0 ) AS alternate_contact_person_key
    , bill_to_customer_key
  FROM dim_customer__convert_boolean_to_string
)

, dim_customer__add_undefined_record AS (
  SELECT
  customer_key
  , customer_name
  , credit_limit
  , standard_discount_percentage
  , payment_days
  , is_statement_sent
  , is_on_credit_hold
  , customer_category_key
  , buying_group_key
  , delivery_city_key
  , primary_contact_person_key
  , alternate_contact_person_key
  , bill_to_customer_key
  FROM dim_customer__handle_null

  UNION ALL
  SELECT
  0 AS customer_key
  , 'Undefined' AS customer_name
  , 0 AS credit_limit
  , 0 AS standard_discount_percentage
  , 0 AS payment_days
  , 'Undefined' AS is_statement_sent
  , 'Undefined' AS is_on_credit_hold
  , 0 AS customer_category_key
  , 0 AS buying_group_key
  , 0 AS delivery_city_key
  , 0 AS primary_contact_person_key
  , 0 AS alternate_contact_person_key
  , 0 AS bill_to_customer_key

  UNION ALL
  SELECT
  -1 AS customer_key
  , 'Invalid' AS customer_name
  , -1 AS credit_limit
  , -1 AS standard_discount_percentage
  , -1 AS payment_days
  , 'Invalid' AS is_statement_sent
  , 'Invalid' AS is_on_credit_hold
  , -1 AS customer_category_key
  , -1 AS buying_group_key
  , -1 AS delivery_city_key
  , -1 AS primary_contact_person_key
  , -1 AS alternate_contact_person_key
  , -1 AS bill_to_customer_key
)

SELECT
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.credit_limit
  , dim_customer.standard_discount_percentage
  , dim_customer.payment_days
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
  , dim_customer.primary_contact_person_key
  , COALESCE ( dim_person_primary.full_name, 'Invalid' ) AS primary_contact_person_name
  , dim_customer.alternate_contact_person_key
  , COALESCE ( dim_person_alternate.full_name, 'Invalid' ) AS alternate_contact_person_name
  , dim_customer.bill_to_customer_key
  , COALESCE ( dim_bill_to_customer.customer_name, 'Invalid' ) AS bill_to_customer_name
FROM dim_customer__add_undefined_record AS dim_customer

LEFT JOIN {{ ref ('stg_dim_customer_category') }} AS dim_customer_category  -- Flatten dim_customer_category to dim_customer
ON dim_customer.customer_category_key = dim_customer_category.customer_category_key

LEFT JOIN {{ ref ('stg_dim_buying_group') }} AS dim_buying_group  -- Flatten dim_buying_group to dim_customer
ON dim_customer.buying_group_key = dim_buying_group.buying_group_key

LEFT JOIN {{ ref ('stg_dim_city') }} AS dim_city -- Flatten delivery city
ON dim_customer.delivery_city_key = dim_city.city_key

LEFT JOIN {{ ref ('dim_person') }} AS dim_person_primary -- Flatten primary contact person
ON dim_customer.primary_contact_person_key = dim_person_primary.person_key

LEFT JOIN {{ ref ('dim_person') }} AS dim_person_alternate -- Flatten alternate contact person
ON dim_customer.alternate_contact_person_key = dim_person_alternate.person_key

LEFT JOIN dim_customer__add_undefined_record AS dim_bill_to_customer -- Flatten bill to customer
ON dim_customer.bill_to_customer_key = dim_bill_to_customer.bill_to_customer_key