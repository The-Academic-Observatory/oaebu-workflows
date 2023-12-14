STRUCT(
    SUM(month.google_books_traffic.Book_Visits_BV_) as Book_Visits_BV_,
    SUM(month.google_books_traffic.BV_with_Pages_Viewed) as BV_with_Pages_Viewed,
    SUM(month.google_books_traffic.Non_Unique_Buy_Clicks) as Non_Unique_Buy_Clicks,
    SUM(month.google_books_traffic.BV_with_Buy_Clicks) as BV_with_Buy_Clicks,
    SUM(month.google_books_traffic.Buy_Link_CTR) as Buy_Link_CTR,
    SUM(month.google_books_traffic.Pages_Viewed) as Pages_Viewed
) as google_books_traffic,