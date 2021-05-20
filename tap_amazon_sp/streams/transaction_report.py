from datetime import datetime, timedelta

from tap_amazon_sp.streams.report import Report
from email.utils import parsedate_tz, mktime_tz
from singer import utils


class TransactionReport(Report):
    report_type = 'GET_DATE_RANGE_FINANCIAL_TRANSACTION_DATA'
    name = "transactions_report"
    api_access_key = None
    replication_method = "FULL_TABLE"
    key_properties = []

    def clean_data(self, data):
        return data[7:]

    def transform_fields(self, obj):
        if "date_time" in obj:
            timestamp = mktime_tz(parsedate_tz(obj["date_time"]))
            utc_dt = datetime(1970, 1, 1) + timedelta(seconds=timestamp)
            obj["date_time"] = utc_dt.ctime()
        return obj
