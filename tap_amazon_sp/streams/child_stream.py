from tap_amazon_sp.context import Context
from tap_amazon_sp.streams.stream import Stream


class ChildStream(Stream):
    parent_name = 'orders'
    parent_id_name = "AmazonOrderId"

    def sync(self):
        selected_parent = Context.stream_objects[self.parent_name]()
        selected_parent.name = self.name
        selected_parent.set_marketplace(self.market_place)

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
