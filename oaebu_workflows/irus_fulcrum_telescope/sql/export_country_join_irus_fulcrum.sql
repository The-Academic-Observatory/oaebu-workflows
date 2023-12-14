LEFT JOIN irus_fulcrum_month_country as irus_fulcrum on month_country.ISBN13 = irus_fulcrum.ISBN13
AND month_country.month = irus_fulcrum.month
AND month_country.alpha2 = irus_fulcrum.alpha2