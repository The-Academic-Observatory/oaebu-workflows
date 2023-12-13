-- Output Schema:
-- name      STRING    NULLABLE
-- value     INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_google_analytics3(items ARRAY < STRUCT < name STRING, value INT64 > >) as (
    ARRAY(
        (
            SELECT
                AS STRUCT name,
                SUM(value) as value,
            FROM
                UNNEST(items)
            GROUP BY
                name
        )
    )
);