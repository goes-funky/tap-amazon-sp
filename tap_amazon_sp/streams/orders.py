from sp_api.api import Orders

from tap_amazon_sp.streams.stream import Stream


class OrdersData(Stream):
    name = "orders_metrics_hourly"
    key_properties = []
    replication_key = "interval"
    replication_method = "FULL_IMPORT"

    def call_api(self, start, end) -> iter:
        orders = Orders()
        data = orders.get_orders(CreatedAfter=start.isoformat(), CreatedBefore=end.isoformat())
        return data.payload