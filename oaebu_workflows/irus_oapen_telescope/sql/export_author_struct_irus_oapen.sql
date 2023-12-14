STRUCT(
    SUM(month.irus_oapen.title_requests) as title_requests,
    SUM(month.irus_oapen.total_item_investigations) as total_item_investigations,
    SUM(month.irus_oapen.total_item_requests) as total_item_requests,
    SUM(month.irus_oapen.unique_item_investigations) as unique_item_investigations,
    SUM(month.irus_oapen.unique_item_requests) as unique_item_requests
) as irus_oapen,