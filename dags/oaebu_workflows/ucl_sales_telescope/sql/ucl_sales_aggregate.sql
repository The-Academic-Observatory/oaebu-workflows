SELECT
  product_id AS ISBN13,
  MIN(published_date) AS published_date,
  title AS title
FROM
  `oaebu-ucl-press.data_export_latest.oaebu_ucl_press_book_list`
GROUP BY
  ISBN13,
  title
