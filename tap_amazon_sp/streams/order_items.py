import datetime

from tap_amazon_sp.streams.child_stream import ChildStream
from tap_amazon_sp.streams.stream import quota_error_handling
from sp_api.api import Orders


class OrderItems(ChildStream):
    name = "order_items"
    replication_method = "INCREMENTAL"
    replication_key = 'PurchaseDate'
    parent_name = 'orders'
    parent_id_name = "AmazonOrderId"
    key_properties = ["OrderItemId"]

    @quota_error_handling
    def call_api(self, **kwargs):
        orders = Orders(marketplace=self.market_place)

        data = orders.get_order_items(kwargs["parent_id"])

        next_token = None
        if "NextToken" in data.payload and data.payload["NextToken"] is not None:
            next_token = data.payload["NextToken"]

        return self.add_parent_id_to_objs(kwargs["parent_id"], data.payload["OrderItems"]), next_token
