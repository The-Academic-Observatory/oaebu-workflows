LEFT JOIN google_books_month_country as google_books ON month_country.ISBN13 = google_books.ISBN13
AND month_country.month = google_books.month
AND month_country.alpha2 = google_books.alpha2