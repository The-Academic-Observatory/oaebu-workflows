STRUCT(
    SUM(month.irus_fulcrum.total_item_investigations) as total_item_investigations,
    SUM(month.irus_fulcrum.total_item_requests) as total_item_requests,
    SUM(month.irus_fulcrum.unique_item_investigations) as unique_item_investigations,
    SUM(month.irus_fulcrum.unique_item_requests) as unique_item_requests
) as irus_fulcrum