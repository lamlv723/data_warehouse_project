SELECT
  person_key AS salesperson_person_key
  , full_name AS salesperson_full_name
FROM {{ ref ('dim_person') }}
WHERE
  is_salesperson = 'Salesperson'
  OR person_key IN ( 0, -1 )