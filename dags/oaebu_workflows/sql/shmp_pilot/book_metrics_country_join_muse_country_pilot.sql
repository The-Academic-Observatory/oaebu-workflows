LEFT JOIN muse_month_country as muse_country_pilot ON month_country.ISBN13 = muse_country_pilot.ISBN13
AND month_country.month = muse_country_pilot.month
AND month_country.country_muse_name = muse_country_pilot.country_name
