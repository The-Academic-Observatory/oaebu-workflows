-- Output Schema:
-- Country_of_Sale      STRING    NULLABLE
-- qty                  INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_google_books_sales(
    items ARRAY < STRUCT < Country_of_Sale STRING,
    qty INT64 > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT Country_of_Sale,
                SUM(qty) as qty,
            FROM
                UNNEST(items)
            GROUP BY
                Country_of_Sale
        )
    )
);