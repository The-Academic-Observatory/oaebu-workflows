-- for muse collections, the alpha2 value is the full country name and needs to be matched on country_muse_name 
LEFT JOIN muse_month_country as muse ON month_country.ISBN13 = muse.ISBN13
AND month_country.month = muse.month
AND month_country.country_muse_name = muse.country_muse_name
