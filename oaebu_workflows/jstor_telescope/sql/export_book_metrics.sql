STRUCT(
    (
        SELECT
            SUM(Total_Item_Requests)
        FROM
            UNNEST(month.jstor_country)
    ) AS Total_Item_Requests
) AS jstor,