CREATE OR REPLACE TABLE workspace.silver.reviews_details AS
SELECT
  CAST(listing_id AS BIGINT) AS listing_id,
  CAST(id AS BIGINT) AS id,
  CAST(date AS DATE) AS date,
  CAST(reviewer_id AS BIGINT) AS reviewer_id,
  CAST(reviewer_name AS STRING) AS reviewer_name,
  CAST(comments AS STRING) AS comments,
  city 
FROM workspace.bronze.reviews_details
WHERE try_cast(id AS BIGINT) IS NOT NULL
AND try_cast(listing_id AS BIGINT) IS NOT NULL;