-- for jstor collections, the alpha2 value is the full country name and needs to be matched on country_jstor_name 
LEFT JOIN jstor_month_country as jstor ON month_country.ISBN13 = jstor.ISBN13
AND month_country.month = jstor.month
AND (
    month_country.alpha2 = jstor.alpha2
    OR month_country.country_jstor_name = jstor.alpha2
)