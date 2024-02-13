STRUCT(
    group_counts(
        ARRAY_CONCAT_AGG(month.google_analytics3.views_total_country)
    ) as views,
    group_counts(
        ARRAY_CONCAT_AGG(month.google_analytics3.downloads_total_country)
    ) as downloads,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics3.downloads_pdf_book_country
        )
    ) as downloads_pdf_book,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics3.downloads_pdf_chapter_country
        )
    ) as downloads_pdf_chapter,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics3.downloads_html_chapter_country
        )
    ) as downloads_html_chapter,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics3.downloads_epub_book_country
        )
    ) as downloads_epub_book,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics3.downloads_epub_chapter_country
        )
    ) as downloads_epub_chapter,
    group_counts(
        ARRAY_CONCAT_AGG(
            month.google_analytics3.downloads_mobi_book_country
        )
    ) as downloads_mobi_book
) as google_analytics3