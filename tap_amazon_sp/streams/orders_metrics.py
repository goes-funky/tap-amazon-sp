from tap_amazon_sp.streams.stream import Stream
from sp_api.api import Sales
from sp_api.base import Granularity

class OrdersMetrics(Stream):
    name = "orders_metrics"
    key_properties = []
    replication_key = "interval"

    def get_data(self, query_params) -> iter:
        sales = Sales()
        data = sales.get_order_metrics(interval=("2018-09-01T10:00:00-07:00", "2018-09-04T12:00:00-07:00"),
                                       granularity=Granularity.DAY)
        return data.payload
