STRUCT(
    SUM(city.title_requests) as title_requests,
    SUM(city.total_item_investigations) as total_item_investigations,
    SUM(city.total_item_requests) as total_item_requests,
    SUM(city.unique_item_investigations) as unique_item_investigations,
    SUM(city.unique_item_requests) as unique_item_requests
) as irus_oapen,