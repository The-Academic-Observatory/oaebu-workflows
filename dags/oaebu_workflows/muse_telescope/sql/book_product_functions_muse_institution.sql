-- Output Schema:
-- institution             STRING    NULLABLE
-- usage     INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_muse_institution(
    items ARRAY < STRUCT < institution STRING,
    usage INT64 > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT institution,
                SUM(usage) as usage,
            FROM
                UNNEST(items)
            GROUP BY
                institution
        )
    )
);
