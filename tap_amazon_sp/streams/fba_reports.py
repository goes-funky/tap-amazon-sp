import copy
from datetime import datetime

import pytz
from dateutil.relativedelta import relativedelta
from tap_amazon_sp.streams.report import Report

"""
You can request up to one month of data in a single report. 
"""

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def split_dates(starting_date: datetime):
    ranges = []
    now = datetime.now(tz=pytz.utc)
    current_date = copy.deepcopy(starting_date)
    while current_date < now:
        new_date = current_date + relativedelta(months=1)

        if new_date > now:
            new_date = now

        ranges.append({
            "start_date": current_date.strftime(DATE_FORMAT),
            "end_date": new_date.strftime(DATE_FORMAT),
        })
        current_date = new_date + relativedelta(seconds=1)
    return ranges


class FBAReport(Report):
    name = ""
    report_type = ""
    replication_method = "INCREMENTAL"
    replication_key = ""
    key_properties = []
    documents = []
    created_report_ids = []

    def sync(self):
        last_imported_datetime = self.get_bookmark(bookmark_key="last_imported_datetime")
        date_ranges = split_dates(last_imported_datetime)
        for date_range in date_ranges:
            report_id = self.create_report(data_start_time=date_range["start_date"], data_end_time=date_range["end_date"])
            document_id = self.wait_on_report_finished(report_id)
            yield from self.push_document(document_id)
            last_imported_datetime = date_range["end_date"]
        self.update_bookmark(last_imported_datetime, "last_imported_datetime")


class AmazonFulfilledShipmentsDataGeneralReport(FBAReport):
    name = "amazon_fulfilled_shipments_data_general_report"
    report_type = "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL"
