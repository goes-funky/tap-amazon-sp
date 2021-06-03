import datetime
import csv
import time

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

    def create_report(self, data_start_time, data_end_time=None):
        singer.log_info("Creating report with report type: " + self.report_type)
        response = self.client_wrapper(self.report_resource.create_report, dataStartTime=data_start_time, dataEndTime=data_end_time, reportType=self.report_type)
        if not response.errors:
            report_id = response.payload["reportId"]
            singer.log_info("Successfully created report with id: " + report_id + " for date range: " + data_start_time + " ==> " + data_end_time)
            return report_id

    def push_document(self, document_id):
        document_obj = self.client_wrapper(self.report_resource.get_report_document, document_id=document_id, decrypt=True).payload
        csv_string = document_obj["document"].splitlines()
        headers = self.clean_headers(csv_string[0])
        yield from csv.DictReader(csv_string[1:], fieldnames=headers, dialect="excel-tab")

    def wait_on_report_finished(self, report_id, throw_on_fatal=True):
        wait_time = 15
        while True:
            time.sleep(wait_time)
            wait_time += 5
            report = self.client_wrapper(self.report_resource.get_report, report_id=report_id)
            status = report.payload["processingStatus"]
            if status == "DONE":
                singer.log_info("Report finished processing.")
                return report.payload["reportDocumentId"]
            elif status in ["IN_PROGRESS", "IN_QUEUE", "PROCESSING"]:
                singer.log_info("Report is still processing. Waiting " + str(wait_time) + " more seconds...")
            elif status in ["FATAL", "CANCELLED"]:
                err_message = "Report processing failed with: " + status + " status, please try again later. Possible reason short date range."
                singer.log_warning(err_message)
                if throw_on_fatal:
                    raise Exception(err_message)
                else:
                    return None

    @quota_error_handling
    def client_wrapper(self, fnc, **kwargs):
        return fnc(**kwargs)

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
        for header in headers.split("\t"):
            new_headers.append(header.replace(" ", "_").replace("-", "_").replace("/", "_").replace("\"", "").replace("(", "").replace(")", "").lower())
        return new_headers
