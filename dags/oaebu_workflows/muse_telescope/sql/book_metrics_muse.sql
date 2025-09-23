STRUCT(
    (
        SELECT
            SUM(usage)
        FROM
            UNNEST(month.muse_country)
    ) AS usage
) AS muse
