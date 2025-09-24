-- Output Schema:
-- country            STRING    NULLABLE
-- usage     INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_muse_country(
    items ARRAY < STRUCT < country STRING,
    usage INT64 > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT country,
                SUM(usage) as usage,
            FROM
                UNNEST(items)
            GROUP BY
                country
        )
    )
);
