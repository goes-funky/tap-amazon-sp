from tap_amazon_sp.streams.stream import Stream, quota_error_handling
from sp_api.api import Sales
from sp_api.base import Granularity


class OrdersMetricsHourly(Stream):
    name = "orders_metrics_hourly"
    key_properties = []
    replication_method = "FULL_IMPORT"
    skip_hour = True

    @quota_error_handling
    def call_api(self, **kwargs) -> iter:
        sales = Sales()
        data = sales.get_order_metrics(interval=(kwargs["start"].isoformat(), kwargs["end"].isoformat()),
                                       granularity=Granularity.HOUR)
        return data.payload
