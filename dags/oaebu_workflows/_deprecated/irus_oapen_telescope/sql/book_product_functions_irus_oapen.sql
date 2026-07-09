-- Output Schema:
-- name                            STRING    NULLABLE
-- code                            STRING    NULLABLE
-- title_requests                  INTEGER   NULLABLE
-- total_item_investigations       INTEGER   NULLABLE
-- total_item_requests             INTEGER   NULLABLE
-- unique_item_investigations      INTEGER   NULLABLE
-- unique_item_requests            INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_irus_country(
    items ARRAY < STRUCT < name STRING,
    code STRING,
    title_requests INT64,
    total_item_investigations INT64,
    total_item_requests INT64,
    unique_item_investigations INT64,
    unique_item_requests INT64 > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT name,
                MAX(code) as code,
                SUM(title_requests) as title_requests,
                SUM(total_item_investigations) as total_item_investigations,
                SUM(total_item_requests) as total_item_requests,
                SUM(unique_item_investigations) as unique_item_investigations,
                SUM(unique_item_requests) as unique_item_requests
            FROM
                UNNEST(items)
            GROUP BY
                name
        )
    )
);

-- Output Schema:
-- latitude                        FLOAT    NULLABLE
-- longitude                       FLOAT    NULLABLE
-- city                            STRING    NULLABLE
-- country_name                    STRING    NULLABLE
-- country_code                    STRING    NULLABLE
-- title_requests                  INTEGER   NULLABLE
-- total_item_investigations       INTEGER   NULLABLE
-- total_item_requests             INTEGER   NULLABLE
-- unique_item_investigations      INTEGER   NULLABLE
-- unique_item_requests            INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_irus_location(
    items ARRAY < STRUCT < latitude FLOAT64,
    longitude FLOAT64,
    city STRING,
    country_name STRING,
    country_code STRING,
    title_requests INT64,
    total_item_investigations INT64,
    total_item_requests INT64,
    unique_item_investigations INT64,
    unique_item_requests INT64 > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT MAX(latitude) as latitude,
                MAX(longitude) as longitude,
                city,
                MAX(country_name) as country_name,
                MAX(country_code) as country_code,
                SUM(title_requests) as title_requests,
                SUM(total_item_investigations) as total_item_investigations,
                SUM(total_item_requests) as total_item_requests,
                SUM(unique_item_investigations) as unique_item_investigations,
                SUM(unique_item_requests) as unique_item_requests
            FROM
                UNNEST(items)
            GROUP BY
                city
        )
    )
);