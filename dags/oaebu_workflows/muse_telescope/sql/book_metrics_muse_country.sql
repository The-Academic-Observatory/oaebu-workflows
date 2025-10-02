STRUCT(
    (
        SELECT
            SUM(book_usage) 
        FROM
            UNNEST(month.muse_country)
    ) AS book_usage,
    (
        SELECT
            SUM(chapter_usage) 
        FROM
            UNNEST(month.muse_country)
    ) AS chapter_usage
) AS muse_country
