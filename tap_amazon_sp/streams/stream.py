import datetime
import functools

import backoff
import singer
from sp_api.api import Sellers
from sp_api.base import SellingApiRequestThrottledException, Marketplaces, SellingApiServerException

from tap_amazon_sp.context import Context
from singer import metrics, utils
import abc

DATE_WINDOW_SIZE = 1

LOGGER = singer.get_logger()

MAX_RETRIES = 10


def retry_handler(details):
    LOGGER.info("Received 500 or retryable error -- Retry %s/%s",
                details['tries'], MAX_RETRIES)


def quota_error_handling(fnc):
    @backoff.on_exception(backoff.expo,
                          SellingApiServerException,
                          # No jitter as we want a constant value
                          jitter=None,
                          max_value=4,
                          max_tries=MAX_RETRIES,
                          on_backoff=retry_handler,
                          )
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
    market_place = Marketplaces.US

    def set_marketplace(self, market_place):
        self.market_place = market_place

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

        # subtracting two minutes because amazon complains if its really close to now
        stop_time = singer.utils.now().replace(microsecond=0) - datetime.timedelta(minutes=2)

        date_window_size = float(Context.config.get("date_window_size", DATE_WINDOW_SIZE))
        while updated_at_min < stop_time:
            updated_at_max = updated_at_min + datetime.timedelta(days=date_window_size)
            if updated_at_max > stop_time:
                updated_at_max = stop_time
            singer.log_info("getting from %s - %s", updated_at_min,
                            updated_at_max)
            next_token = None
            while True:
                objects, next_token = self.call_api(start=updated_at_min, end=updated_at_max, nextToken=next_token)

                for object in objects:
                    yield object

                if next_token is None:
                    break

            skip_time = datetime.timedelta(seconds=1)
            if self.skip_hour:
                skip_time = datetime.timedelta(hours=1)

            updated_at_min = updated_at_max + skip_time

            self.update_bookmark(utils.strftime(updated_at_min))

    # implemented by each stream class, returns data and next token if there is
    @abc.abstractmethod
    def call_api(self, **kwargs):
        return [], None
