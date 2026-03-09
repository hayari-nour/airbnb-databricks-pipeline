CREATE OR REPLACE TABLE workspace.silver.neighbourhoods AS
SELECT
    feature.properties.neighbourhood AS neighbourhood,
    feature.properties.neighbourhood_group AS neighbourhood_group,
    b.city,
    feature.type AS feature_type,
    feature.geometry.type AS geometry_type,
    st_geomfromgeojson(
        to_json(feature.geometry)
    ) AS geom
FROM workspace.bronze.neighbourhoods b
LATERAL VIEW EXPLODE(b.features) AS feature;