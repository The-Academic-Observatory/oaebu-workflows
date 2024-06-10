LEFT JOIN muse_month_country as muse_country_pilot ON month_country.ISBN13 = muse_country_pilot.ISBN13
AND month_country.month = muse_country_pilot.month
AND (month_country.country_iso_name = muse_country_pilot.country_name OR month_country.country_wikipedia_name = muse_country_pilot.country_name)
