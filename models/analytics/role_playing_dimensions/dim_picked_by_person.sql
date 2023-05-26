SELECT
  person_key AS picked_by_person_key
  , full_name AS picked_by_full_name
  , is_employee AS is_employee
FROM {{ ref ('dim_person') }}