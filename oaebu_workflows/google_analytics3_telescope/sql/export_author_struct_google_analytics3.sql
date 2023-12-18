STRUCT(
    group_counts(
        ARRAY_CONCAT_AGG(month.google_analytics.views_total_country)
    ) as views_total_country,
    group_counts(
        ARRAY_CONCAT_AGG(month.google_analytics.downloads_total_country)
    ) as downloads,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics.downloads_pdf_book_country
        )
    ) as downloads_pdf_book,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics.downloads_pdf_chapter_country
        )
    ) as downloads_pdf_chapter,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics.downloads_html_chapter_country
        )
    ) as downloads_html_chapter,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics.downloads_epub_book_country
        )
    ) as downloads_epub_book,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics.downloads_epub_chapter_country
        )
    ) as downloads_epub_chapter,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics.downloads_mobi_book_country
        )
    ) as downloads_mobi_book
) as google_analytics