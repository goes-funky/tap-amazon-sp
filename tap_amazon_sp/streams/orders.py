import datetime

from sp_api.api import Orders

from tap_amazon_sp.streams.stream import Stream
import singer


class OrdersData(Stream):
    name = "orders"
    api_access_key = "Orders"
    key_properties = []
    replication_key = "interval"
    replication_method = "INCREMENTAL"

    def call_api(self, **kwargs):
        orders = Orders()
        query_params = self.get_query_params(kwargs)

        data = orders.get_orders(**query_params)

        next_token = None
        if "NextToken" in data.payload and data.payload["NextToken"] is not None:
            next_token = data.payload["NextToken"]

        return data.payload["Orders"], next_token

    @staticmethod
    def get_query_params(kwargs):
        query_params = {
            "CreatedAfter": kwargs["start"].replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
            "CreatedBefore": kwargs["end"].replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        }

        if "nextToken" in kwargs and kwargs["nextToken"] is not None:
            query_params["NextToken"] = kwargs["nextToken"]

        return query_params
