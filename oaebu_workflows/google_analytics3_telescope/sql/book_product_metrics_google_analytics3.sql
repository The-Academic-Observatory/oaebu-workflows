STRUCT(
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(month.google_analytics3.views_total_country)
    ) AS views,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(month.google_analytics3.downloads_total_country)
    ) AS downloads,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics3.downloads_pdf_book_country
            )
    ) AS downloads_pdf_book,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics3.downloads_pdf_chapter_country
            )
    ) AS downloads_pdf_chapter,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics3.downloads_html_chapter_country
            )
    ) AS downloads_html_chapter,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics3.downloads_epub_book_country
            )
    ) AS downloads_epub_book,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics3.downloads_epub_chapter_country
            )
    ) AS downloads_epub_chapter,
    (
        SELECT
            SUM(value)
        FROM
            UNNEST(
                month.google_analytics3.downloads_mobi_book_country
            )
    ) AS downloads_mobi_book
) AS google_analytics3