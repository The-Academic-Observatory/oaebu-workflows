LEFT JOIN google_analytics_page_views_month_country as google_page_views ON month_country.ISBN13 = google_page_views.ISBN13
AND month_country.month = google_page_views.month
AND month_country.alpha2 = google_page_views.country_code
LEFT JOIN google_analytics_downloads_total_month_country as google_downloads ON month_country.ISBN13 = google_downloads.ISBN13
AND month_country.month = google_downloads.month
AND month_country.alpha2 = google_downloads.country_code
LEFT JOIN google_analytics_downloads_pdf_book_month_country as google_downloads_pdf_book ON month_country.ISBN13 = google_downloads_pdf_book.ISBN13
AND month_country.month = google_downloads_pdf_book.month
AND month_country.alpha2 = google_downloads_pdf_book.country_code
LEFT JOIN google_analytics_downloads_pdf_chapter_month_country as google_downloads_pdf_chapter ON month_country.ISBN13 = google_downloads_pdf_chapter.ISBN13
AND month_country.month = google_downloads_pdf_chapter.month
AND month_country.alpha2 = google_downloads_pdf_chapter.country_code
LEFT JOIN google_analytics_downloads_html_chapter_month_country as google_downloads_html_chapter ON month_country.ISBN13 = google_downloads_html_chapter.ISBN13
AND month_country.month = google_downloads_html_chapter.month
AND month_country.alpha2 = google_downloads_html_chapter.country_code
LEFT JOIN google_analytics_downloads_epub_book_month_country as google_downloads_epub_book ON month_country.ISBN13 = google_downloads_epub_book.ISBN13
AND month_country.month = google_downloads_epub_book.month
AND month_country.alpha2 = google_downloads_epub_book.country_code
LEFT JOIN google_analytics_downloads_epub_chapter_month_country as google_downloads_epub_chapter ON month_country.ISBN13 = google_downloads_epub_chapter.ISBN13
AND month_country.month = google_downloads_epub_chapter.month
AND month_country.alpha2 = google_downloads_epub_chapter.country_code
LEFT JOIN google_analytics_downloads_mobi_book_month_country as google_downloads_mobi_book ON month_country.ISBN13 = google_downloads_mobi_book.ISBN13
AND month_country.month = google_downloads_mobi_book.month
AND month_country.alpha2 = google_downloads_mobi_book.country_code