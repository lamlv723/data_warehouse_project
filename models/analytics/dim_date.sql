WITH dim_date__generate_date AS (
  SELECT *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2030-12-31', INTERVAL 1 DAY)) AS date
)

SELECT
  date,
  FORMAT_DATE('%A', date) AS day_of_week -- Get weekday name 
  , FORMAT_DATE('%a', date) AS day_of_week_short -- Get weekday name short 
  , CASE 
      WHEN FORMAT_DATE('%A', date) IN ('Sunday', 'Saturday') THEN 'Weekend'
      ELSE 'Weekday'
    END AS day_is_weekday
  , DATE_TRUNC (date, MONTH) AS year_month
  , FORMAT_DATE('%B', date) AS month --Get month name
  , DATE_TRUNC (date, YEAR) AS year
  , EXTRACT(YEAR FROM date) AS year_number
FROM dim_date__generate_date