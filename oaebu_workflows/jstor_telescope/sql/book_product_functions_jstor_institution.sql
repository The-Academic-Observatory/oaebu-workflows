-- Output Schema:
-- Institution             STRING    NULLABLE
-- Total_Item_Requests     INTEGER   NULLABLE
CREATE TEMP FUNCTION group_items_jstor_institution(
    items ARRAY < STRUCT < Institution STRING,
    Total_Item_Requests INT64 > >
) as (
    ARRAY(
        (
            SELECT
                AS STRUCT Institution,
                SUM(Total_Item_Requests) as Total_Item_Requests,
            FROM
                UNNEST(items)
            GROUP BY
                Institution
        )
    )
);