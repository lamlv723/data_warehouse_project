WITH dim_country__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__countries`
)

, dim_country__rename_column AS (
  SELECT
    country_id AS country_key
    , country_name AS country_name
    , country_type AS country_type
    , continent AS continent
    , region AS region
    , subregion AS subregion
  FROM dim_country__source
)

, dim_country__cast_type AS (
  SELECT
    CAST ( country_key AS INTEGER ) AS country_key
    , CAST ( country_name AS STRING ) AS country_name
    , CAST ( country_type AS STRING ) AS country_type
    , CAST ( continent AS STRING ) AS continent
    , CAST ( region AS STRING ) AS region
    , CAST ( subregion AS STRING ) AS subregion
  FROM dim_country__rename_column
)

, dim_country__add_undefined_record AS (
  SELECT
    country_key
    , country_name
    , country_type
    , continent
    , region
    , subregion
  FROM dim_country__cast_type

  UNION ALL
  SELECT
  0 AS country_key
  , 'Undefined' AS country_name
  , 'Undefined' AS country_type
  , 'Undefined' AS continent
  , 'Undefined' AS region
  , 'Undefined' AS subregion

  UNION ALL
  SELECT
  -1 AS country_key
  , 'Invalid' AS country_name
  , 'Invalid' AS country_type
  , 'Invalid' AS continent
  , 'Invalid' AS region
  , 'Invalid' AS subregion
)

SELECT
  country_key
  , country_name
  , country_type
  , continent
  , region
  , subregion
FROM dim_country__add_undefined_record
