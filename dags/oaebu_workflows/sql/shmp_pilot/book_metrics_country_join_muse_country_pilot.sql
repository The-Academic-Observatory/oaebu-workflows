-- for muse collections, the alpha2 value is the full country name and needs to be matched on country_muse_name 
LEFT JOIN muse_month_country as muse_country_pilot ON month_country.ISBN13 = muse_country_pilot.ISBN13
AND month_country.month = muse_country_pilot.month
AND month_country.alpha2 = muse_country_pilot.alpha2
