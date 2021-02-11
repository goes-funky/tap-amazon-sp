import datetime

from tap_amazon_sp.streams.stream import Stream, quota_error_handling
from sp_api.api import Orders
from tap_amazon_sp.context import Context


class OrderItems(Stream):
    name = "order_items"
    replication_method = "INCREMENTAL"
    replication_key = 'PurchaseDate'
    parent_name = 'orders'
    parent_id_name = "AmazonOrderId"
    key_properties = ["OrderItemId"]

    @quota_error_handling
    def call_api(self, **kwargs):
        orders = Orders()

        data = orders.get_order_items(kwargs["parent_id"])

        next_token = None
        if "NextToken" in data.payload and data.payload["NextToken"] is not None:
            next_token = data.payload["NextToken"]

        return self.add_parent_id_to_objs(kwargs["parent_id"], data.payload["OrderItems"]), next_token

    def sync(self):
        selected_parent = Context.stream_objects[self.parent_name]()
        selected_parent.name = self.name
        next_token = None
        for parent_obj in selected_parent.sync():
            while True:
                objects, next_token = self.call_api(parent_id=parent_obj[self.parent_id_name], nextToken=next_token)

                for object in objects:
                    yield object

                if next_token is None:
                    break

    def add_parent_id_to_objs(self, parent_id, objs):
        data = []
        for obj in objs:
            obj[self.parent_id_name] = parent_id
            data.append(obj)

        return data
