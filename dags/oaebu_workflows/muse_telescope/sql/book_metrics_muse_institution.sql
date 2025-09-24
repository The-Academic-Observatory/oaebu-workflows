STRUCT(
    (
        SELECT
            SUM(usage)
        FROM
            UNNEST(month.muse_institution)
    ) AS usage
) AS muse_country
