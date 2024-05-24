-- Output Schema:
-- Country_name            STRING    NULLABLE
-- Total_Item_Requests     INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_muse_country_pilot(
    items ARRAY < STRUCT < Country_name STRING,
    Total_Item_Requests INT64 > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT Country_name,
                SUM(Total_Item_Requests) as Total_Item_Requests,
            FROM
                UNNEST(items)
            GROUP BY
                Country_name
        )
    )
);
