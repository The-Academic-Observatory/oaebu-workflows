-- Output Schema:
-- institution             STRING    NULLABLE
-- usage     INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_muse_institution(
    items ARRAY < STRUCT < institution STRING,
    usage INT64, type STRING > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT institution,
                SUM(CASE WHEN type = "full book" THEN usage ELSE 0 END) as book_usage,
                SUM(CASE WHEN type = "chapter" THEN usage ELSE 0 END) as chapter_usage
            FROM
                UNNEST(items)
            GROUP BY
                institution
        )
    )
);
