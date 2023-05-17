WITH dim_person__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column AS (
  SELECT
    person_id AS person_key
    , full_name AS full_name
    , preferred_name AS preferred_name
    , is_employee AS is_employee
    , is_salesperson AS is_salesperson
  FROM dim_person__source
)

, dim_person__cast_type AS (
  SELECT
    CAST ( person_key AS INTEGER ) AS person_key
    , CAST ( full_name AS STRING ) AS full_name
    , CAST ( preferred_name AS STRING ) AS preferred_name
    , CAST ( is_employee AS BOOLEAN ) AS is_employee_boolean
    , CAST ( is_salesperson AS BOOLEAN ) AS is_salesperson_boolean
  FROM dim_person__rename_column
)

, dim_person__change_boolean_cols_to_string AS (
  SELECT 
  *
  , CASE
      WHEN is_employee_boolean IS TRUE THEN 'Employee'
      WHEN is_employee_boolean IS FALSE THEN 'Not an employee'
      WHEN is_employee_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid'
    END AS is_employee
  , CASE
      WHEN is_salesperson_boolean IS TRUE THEN 'Salesperson'
      WHEN is_salesperson_boolean IS FALSE THEN 'Not a salesperson'
      WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid'
    END AS is_salesperson
  FROM dim_person__cast_type

)

, dim_person__add_undefined_record AS (
  SELECT
    person_key
    , full_name
    , preferred_name
    , is_employee
    , is_salesperson
  FROM dim_person__change_boolean_cols_to_string

  -- Lesson-0110: Handling null 
  UNION ALL
  SELECT
    0 AS person_key
    , 'Undefined' AS full_name
    , 'Undefined' AS preferred_name
    , 'Undefined' AS is_employee
    , 'Undefined' AS is_salesperson

  UNION ALL
  SELECT
    -1 AS person_key
    , 'Invalid' AS full_name
    , 'Invalid' AS preferred_name
    , 'Invalid' AS is_employee
    , 'Invalid' AS is_salesperson
)

, dim_person__handle_null AS (
  SELECT
    person_key
    , COALESCE ( full_name, 'Undefined' ) AS full_name
    , COALESCE ( preferred_name, 'Undefined' ) AS preferred_name
    , is_employee -- Already handled
    , is_salesperson -- Already handled
  FROM dim_person__add_undefined_record
)

SELECT
  person_key
  , full_name
  , preferred_name
  , is_employee
  , is_salesperson
FROM dim_person__handle_null
ORDER BY person_key