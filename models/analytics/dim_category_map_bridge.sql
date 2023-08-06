WITH dim_category_map_bridge__source AS (
  SELECT *
  FROM {{ref('dim_category')}}
)

SELECT
  category_key_level_1 AS parent_category_key
  , category_level_1 AS parent_category_name
  , category_key AS child_category_key
  , category_name AS child_category_name
FROM dim_category_map_bridge__source

UNION ALL
SELECT
  category_key_level_2 AS parent_category_key
  , category_level_2 AS parent_category_name
  , category_key AS child_category_key
  , category_name AS child_category_name
FROM dim_category_map_bridge__source
WHERE category_key_level_2 NOT IN (-1, 0)

UNION ALL
SELECT
  category_key_level_3 AS parent_category_key
  , category_level_3 AS parent_category_name
  , category_key AS child_category_key
  , category_name AS child_category_name
FROM dim_category_map_bridge__source
WHERE category_key_level_3 NOT IN (-1, 0)

UNION ALL
SELECT
  category_key_level_4 AS parent_category_key
  , category_level_4 AS parent_category_name
  , category_key AS child_category_key
  , category_name AS child_category_name
FROM dim_category_map_bridge__source
WHERE category_key_level_4 NOT IN (-1, 0)
ORDER BY 1