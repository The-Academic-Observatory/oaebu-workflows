LEFT JOIN irus_oapen_month_country as irus_oapen ON month_country.ISBN13 = irus_oapen.ISBN13
AND month_country.month = irus_oapen.month
AND month_country.alpha2 = irus_oapen.alpha2