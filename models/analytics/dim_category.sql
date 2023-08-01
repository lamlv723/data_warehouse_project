WITH dim_category__source AS (
  SELECT *
  FROM {{ref('stg_dim_category')}}
)

, dim_category__level_1 AS (
  SELECT
    *
    , category_key AS category_key_level_1
    , category_name AS category_level_1
    , 0 AS category_key_level_2
    , 'Undefined' AS category_level_2
    , 0 AS category_key_level_3
    , 'Undefined' AS category_level_3
    , 0 AS category_key_level_4
    , 'Undefined' AS category_level_4
  FROM dim_category__source
  WHERE category_level = 1
)

, dim_category__level_2 AS (
  SELECT
    *
    , parent_category_key AS category_key_level_1
    , parent_category_name AS category_level_1
    , category_key AS category_key_level_2
    , category_name AS category_level_2
    , 0 AS category_key_level_3
    , 'Undefined' AS category_level_3
    , 0 AS category_key_level_4
    , 'Undefined' AS category_level_4
  FROM dim_category__source
  WHERE category_level = 2
)

, dim_category__level_3 AS (
  SELECT
    level_3.*
    , level_2.parent_category_key AS category_key_level_1
    , level_2.parent_category_name AS category_level_1
    , level_3.parent_category_key AS category_key_level_2
    , level_3.parent_category_name AS category_level_2
    , level_3.category_key AS category_key_level_3
    , level_3.category_name AS category_level_3
    , 0 AS category_key_level_4
    , 'Undefined' AS category_level_4
  FROM dim_category__source AS level_3
  LEFT JOIN dim_category__source AS level_2
    ON level_3.parent_category_key = level_2.category_key
  WHERE level_3.category_level = 3
)

, dim_category__level_4 AS (
  SELECT
    level_4.*
    , level_2.parent_category_key AS category_key_level_1
    , level_2.parent_category_name AS category_level_1
    , level_3.parent_category_key AS category_key_level_2
    , level_3.parent_category_name AS category_level_2
    , level_4.parent_category_key AS category_key_level_3
    , level_4.parent_category_name AS category_level_3
    , level_4.category_key AS category_key_level_4
    , level_4.category_name AS category_level_4
  FROM dim_category__source AS level_4
  LEFT JOIN dim_category__source AS level_3
    ON level_4.parent_category_key = level_3.category_key

  LEFT JOIN dim_category__source AS level_2
    ON level_3.parent_category_key = level_2.category_key
    
  WHERE level_4.category_level = 4
)

, dim_category__union AS (
  SELECT * FROM dim_category__level_1

  UNION ALL
  SELECT * FROM dim_category__level_2

  UNION ALL
  SELECT * FROM dim_category__level_3

  UNION ALL
  SELECT * FROM dim_category__level_4
)

SELECT
  category_key
  , category_name
  , category_level
  , parent_category_key
  , parent_category_name
  , category_key_level_1
  , category_level_1
  , category_key_level_2
  , category_level_2
  , category_key_level_3
  , category_level_3
  , category_key_level_4
  , category_level_4
FROM dim_category__union