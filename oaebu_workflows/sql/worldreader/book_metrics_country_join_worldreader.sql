LEFT JOIN worldreader_month_country as worldreader ON month_country.ISBN13 = worldreader.ISBN13
AND month_country.month = worldreader.month
AND month_country.alpha2 = worldreader.country_code