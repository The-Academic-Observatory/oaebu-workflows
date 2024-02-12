LEFT JOIN ucl_discovery_month_country as ucl_discovery ON month_country.ISBN13 = ucl_discovery.ISBN13
AND month_country.month = ucl_discovery.month
AND month_country.alpha2 = ucl_discovery.alpha2