import datetime
import functools

import backoff
import singer
from sp_api.base import SellingApiRequestThrottledException

from tap_amazon_sp.context import Context
from singer import metrics, utils
import abc

DATE_WINDOW_SIZE = 1


def quota_error_handling(fnc):
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          # No jitter as we want a constant value
                          jitter=None,
                          max_value=4
                          )
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)

    return wrapper


def is_not_status_code_fn(status_code):
    def gen_fn(exc):
        if getattr(exc, 'code', None) and exc.code not in status_code:
            return True
        # Retry other errors up to the max
        return False

    return gen_fn



class Stream:
    # Used for bookmarking and stream identification. Is overridden by
    # subclasses to change the bookmark key.
    name = None
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at'
    key_properties = ['id']
    # Controls which SDK object we use to call the API by default.
    replication_object = None
    # Status parameter override option
    status_key = None
    skip_hour = False

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state,
                                        # name is overridden by some substreams
                                        self.name,
                                        self.replication_key)
                    or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, bookmark_value, bookmark_key=None):
        # NOTE: Bookmarking can never be updated to not get the most
        # recent thing it saw the next time you run, because the querying
        # only allows greater than or equal semantics.

        singer.write_bookmark(
            Context.state,
            # name is overridden by some substreams
            self.name,
            bookmark_key or self.replication_key,
            bookmark_value
        )
        singer.write_state(Context.state)

    def sync(self):
        updated_at_min = self.get_bookmark()
        stop_time = singer.utils.now().replace(microsecond=0)
        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))
        while updated_at_min < stop_time:
            updated_at_max = updated_at_min + datetime.timedelta(days=date_window_size)
            if updated_at_max > stop_time:
                updated_at_max = stop_time
            singer.log_info("getting from %s - %s", updated_at_min,
                            updated_at_max)

            objects = self.call_api(start=updated_at_min, end=updated_at_max)

            for object in objects:
                yield object

            Context.state.get('bookmarks', {}).get(self.name, {}).pop('since_id', None)
            self.update_bookmark(utils.strftime(updated_at_max))

            skip_time = datetime.timedelta(seconds=1)
            if self.skip_hour:
                skip_time = datetime.timedelta(hours=1)

            updated_at_min = updated_at_max + skip_time

    # implemented by each stream class
    @abc.abstractmethod
    def call_api(self, **kwargs) -> iter:
        return []
