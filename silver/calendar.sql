CREATE OR REPLACE TABLE workspace.silver.calendar AS
SELECT
  CAST(listing_id AS bigint) AS listing_id,
  CAST(date AS DATE) AS date,
  CAST(available AS BOOLEAN) AS available,
  CAST(price AS INT) AS price,
  CAST(adjusted_price AS INT) AS adjusted_price,
  CAST(minimum_nights AS INT) AS minimum_nights,
  CAST(maximum_nights AS INT) AS maximum_nights,
  city
FROM
  workspace.bronze.calendar;