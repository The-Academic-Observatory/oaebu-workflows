LEFT JOIN jstor_country_pilot_month_country as jstor_country_pilot ON month_country.ISBN13 = jstor_country_pilot.ISBN13
AND month_country.month = jstor_country_pilot.month
AND (
    month_country.alpha2 = jstor_country_pilot.alpha2
    OR month_country.country_jstor_name = jstor_country_pilot.alpha2
)
