-- Output Schema:
-- name                            STRING    NULLABLE
-- code                            STRING    NULLABLE
-- name                            INTEGER   NULLABLE
-- downloads                       INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_worldreader_country(
    items ARRAY < STRUCT < country_name STRING,
    code STRING,
    downloads INT64 > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT country_name,
                MAX(code) as country_code,
                SUM(downloads) as downloads
            FROM
                UNNEST(items)
            GROUP BY
                country_name
        )
    )
);