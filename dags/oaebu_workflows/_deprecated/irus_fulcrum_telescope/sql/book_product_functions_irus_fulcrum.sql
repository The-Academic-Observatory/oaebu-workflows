-- Output Schema:
-- name                            STRING    NULLABLE
-- code                            STRING    NULLABLE
-- total_item_investigations       INTEGER   NULLABLE
-- total_item_requests             INTEGER   NULLABLE
-- unique_item_investigations      INTEGER   NULLABLE
-- unique_item_requests            INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_irus_fulcrum_country(
    items ARRAY < STRUCT < name STRING,
    code STRING,
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