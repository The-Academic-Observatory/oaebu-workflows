google_books_month_country as (
    SELECT
        ISBN13,
        month,
        Country_of_Sale as alpha2,
        google_books_sales.qty
    FROM
        months,
        UNNEST(google_books_sales.countries)
)