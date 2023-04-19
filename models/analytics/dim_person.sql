WITH dim_person__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column AS (
  SELECT
    person_id AS person_key
    , full_name AS full_name
  FROM dim_person__source
)

, dim_person__cast_type AS (
  SELECT
    CAST ( person_key AS INTEGER ) AS person_key
    , CAST ( full_name AS STRING ) AS full_name
  FROM dim_person__rename_column
)

, dim_person__add_key_for_null AS (
  SELECT
    person_key
    , full_name
  FROM dim_person__cast_type

  UNION ALL
  
  SELECT
    0 AS person_key
    , 'Undefined' AS full_name
)

SELECT
  person_key
  , full_name
FROM dim_person__add_key_for_null