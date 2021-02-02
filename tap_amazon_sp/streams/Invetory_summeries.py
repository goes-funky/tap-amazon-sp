from tap_amazon_sp.streams.stream import Stream
from sp_api.api import Inventories


class InventorySummary(Stream):
    replication_method = "FULL_IMPORT"
    key_properties = []

    def call_api(self, **kwargs) -> iter:
        inventories = Inventories()
        params = {}
        if "next_token" in kwargs:
            params = {
                "next_token": kwargs["next_token"]
            }

        data = inventories.get_inventory_summary_marketplace(**params).to_dict()
        return data

    def sync(self):
        next_token = None
        params = {}
        while True:
            data = self.call_api(**params)

            for object in data["payload"]:
                yield object

            if "pagination" in data:
                params = {"next_token": data["pagination"]["nextToken"]}
            else:
                break


