STRUCT(
    (
        SELECT
            SUM(Total_Item_Requests)
        FROM
            UNNEST(month.muse_country_pilot)
    ) AS Total_Item_Requests
) AS muse