import datetime
import csv
from sp_api.api import Reports
from sp_api.base import Client, Marketplaces

from tap_amazon_sp.streams.stream import Stream, quota_error_handling
import singer

from singer import metrics, utils


class Report(Stream):
    def call_api(self, **kwargs):
        pass

    report_type = 'GET_DATE_RANGE_FINANCIAL_TRANSACTION_DATA'

    def __init__(self):
        self.report_resource = Reports(marketplace=self.market_place)

    def get_latest_report_doc_id(self, payload):
        latest_date = utils.strptime_with_tz(payload[0]["createdTime"])
        latest_document_id = payload[0]["reportDocumentId"]
        for report in payload:
            if utils.strptime_with_tz(report["createdTime"]) > latest_date:
                latest_date = utils.strptime_with_tz(report["createdTime"])
                latest_document_id = report["reportDocumentId"]
        return latest_document_id

    def sync(self):
        reports = self.report_resource.get_reports(reportTypes=[self.report_type])
        reports_payload = reports.payload
        if len(reports_payload) == 0:
            return []
        document_id = self.get_latest_report_doc_id(reports_payload)
        document_obj = self.report_resource.get_report_document(document_id, decrypt=True).payload
        csv_string = document_obj["document"].splitlines()
        clean_data = self.clean_data(csv_string)
        headers = self.clean_headers(clean_data[0])
        yield from csv.DictReader(clean_data[1:], fieldnames=headers)

    def clean_data(self, data):
        return data

    def clean_headers(self, headers):
        new_headers = []
        for header in headers.split(","):
            new_headers.append(header.replace(" ", "_").replace("/", "_").replace("\"", ""))
        return new_headers
