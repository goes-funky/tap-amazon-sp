import datetime
import functools

import backoff
import singer
from sp_api.api import Sellers
from sp_api.base import SellingApiRequestThrottledException, Marketplaces, SellingApiServerException, SellingApiForbiddenException

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
                          max_value=32,
                          max_tries=MAX_RETRIES,
                          on_backoff=retry_handler,
                          )
    @backoff.on_exception(backoff.expo,
                          SellingApiRequestThrottledException,
                          # No jitter as we want a constant value
                          jitter=None,
                          max_value=32
                          )
    @backoff.on_exception(backoff.expo,
                          SellingApiForbiddenException,
                          # No jitter as we want a constant value
                          jitter=None,
                          on_backoff=retry_handler,
                          max_tries=MAX_RETRIES,
                          max_value=32
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
    logger = singer.get_logger()
    market_place = Marketplaces.US

    def set_marketplace(self, market_place):
        self.market_place = market_place

    def get_bookmark(self, bookmark_key=None):
        bookmark = (singer.get_bookmark(Context.state,
                                        # name is overridden by some substreams
                                        self.name,
                                        bookmark_key or self.replication_key)
                    or Context.config["start_date"])
        return bookmark if bookmark_key else utils.strptime_with_tz(bookmark)

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

        next_token = None
        new_bookmark = utils.strftime(updated_at_min, utils.DATETIME_PARSE)
        self.logger.info("Getting data from " + new_bookmark)
        page = 1
        while True:
            objects, next_token = self.call_api(start=updated_at_min, nextToken=next_token)
            self.logger.info("Retrieved page " + str(page))
            for object in objects:
                new_bookmark = object.get(self.replication_key, new_bookmark)
                yield object

            if next_token is None:
                break
            page += 1

        self.update_bookmark(utils.strftime(utils.strptime_with_tz(new_bookmark) + datetime.timedelta(seconds=1), utils.DATETIME_PARSE))

    # implemented by each stream class, returns data and next token if there is
    @abc.abstractmethod
    def call_api(self, **kwargs):
        return [], None

    def transform_fields(self, obj):
        return obj
