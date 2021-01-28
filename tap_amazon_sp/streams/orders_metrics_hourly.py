
from tap_amazon_sp.streams.stream import Stream
from sp_api.api import Sales
from sp_api.base import Granularity


class OrdersMetricsHourly(Stream):
    name = "orders_metrics_hourly"
    key_properties = []
    replication_key = "interval"
    replication_method = "FULL_IMPORT"

    def call_api(self, start, end) -> iter:
        sales = Sales()
        data = sales.get_order_metrics(interval=(start.isoformat(), end.isoformat()),
                                       granularity=Granularity.HOUR)
        return data.payload
