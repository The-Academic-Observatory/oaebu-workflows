STRUCT(
    (
        SELECT
            SUM(book_usage) as book_usage,
        FROM
            UNNEST(month.muse_institution)
    ) AS book_usage,
    (
        SELECT
            SUM(chapter_usage) as chapter_usage,
        FROM
            UNNEST(month.muse_institution)
    ) AS chapter_usage
) AS muse_institution
