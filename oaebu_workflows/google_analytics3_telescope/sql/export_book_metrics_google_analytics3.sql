STRUCT(
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(month.google_analytics.views_total_country)
    ) AS page_views,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(month.google_analytics.downloads_total_country)
    ) AS downloads,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics.downloads_pdf_book_country
            )
    ) AS downloads_pdf_book,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics.downloads_pdf_chapter_country
            )
    ) AS downloads_pdf_chapter,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics.downloads_html_chapter_country
            )
    ) AS downloads_html_chapter,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics.downloads_epub_book_country
            )
    ) AS downloads_epub_book,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics.downloads_epub_chapter_country
            )
    ) AS downloads_epub_chapter,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics.downloads_mobi_book_country
            )
    ) AS downloads_mobi_book
) AS google_analytics