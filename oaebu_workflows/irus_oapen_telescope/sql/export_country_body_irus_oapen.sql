irus_oapen_month_country as (
    SELECT
        ISBN13,
        month,
        code as alpha2,
        title_requests,
        total_item_investigations,
        total_item_requests,
        unique_item_investigations,
        unique_item_requests
    FROM
        months,
        UNNEST(irus_oapen.country)
),