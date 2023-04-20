WITH dim_date__generate_date AS (
  SELECT *
  FROM
    UNNEST ( GENERATE_DATE_ARRAY ( '2010-01-01', '2030-12-31', INTERVAL 1 DAY ) ) AS date
)

, dim_date__add_custom_column AS (
  SELECT
    * 
    , FORMAT_DATE ( '%A', date ) AS day_of_week -- Get weekday name 
    , FORMAT_DATE ( '%a', date ) AS day_of_week_short -- Get weekday name short 
    , DATE_TRUNC  ( date, MONTH ) AS year_month
    , FORMAT_DATE ( '%B', date ) AS month --Get month name
    , DATE_TRUNC  ( date, YEAR ) AS year
    , EXTRACT ( YEAR FROM date ) AS year_number
  FROM dim_date__generate_date
)

, dim_date__check_weekend AS (
  SELECT
  *
  , CASE 
      WHEN day_of_week_short IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN 'Weekday'
      WHEN day_of_week_short IN ('Sat', 'Sun') THEN 'Weekend'
      ELSE 'Invalid'
    END AS is_weekday_or_weekend
  FROM dim_date__add_custom_column
)

SELECT
  date
  , day_of_week
  , day_of_week_short
  , is_weekday_or_weekend
  , year_month
  , month
  , year
  , year_number
FROM dim_date__check_weekend