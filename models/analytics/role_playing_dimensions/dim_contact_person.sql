SELECT
  person_key AS dim_contact_person_key
  , full_name AS dim_contact_person_name
FROM {{ ref ('dim_person') }}