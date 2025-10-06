-- Output Schema:
-- country            STRING    NULLABLE
-- usage     INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_muse_country(
    items ARRAY < STRUCT < country STRING,
    usage INT64, type STRING > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT country,
                SUM(CASE WHEN type = "full book" THEN usage ELSE 0 END) as book_usage,
                SUM(CASE WHEN type = "chapter" THEN usage ELSE 0 END) as chapter_usage
            FROM
                UNNEST(items)
            GROUP BY
                country
        )
    )
);
