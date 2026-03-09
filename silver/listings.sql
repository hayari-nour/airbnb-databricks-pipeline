Create or replace table workspace.silver.listing as

select 
cast(id as bigint) as id,
name,
cast(host_id as bigint) as host_id,
host_name,
neighbourhood_group,
neighbourhood,
cast(latitude as double) as latitude,
cast(longitude as double) as longitude,
room_type,
cast(price as int) as price,
cast(minimum_nights as int) as minimum_nights,
cast(number_of_reviews as int) as number_of_reviews,
last_review,
cast(reviews_per_month as double) as reviews_per_month,
cast(calculated_host_listings_count as int) as calculated_host_listings_count,
cast(availability_365 as int) as availability_365,
cast(number_of_reviews_ltm as int) as number_of_reviews_ltm,
license,
city
from workspace.bronze.listing
WHERE try_cast(id AS BIGINT) IS NOT NULL
AND try_cast(host_id AS BIGINT) IS NOT NULL;